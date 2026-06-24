// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	otellog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/metric"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
	"go.opentelemetry.io/otel/trace"
)

const (
	endpointGRPC = "elastic-agent:4317"
	endpointHTTP = "elastic-agent:4318"
	count        = 50
)

func main() {
	// Wait for SIGHUP from elastic-package (agent ready signal).
	log.Println("waiting for SIGHUP...")
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGHUP)
	<-sig
	log.Println("received SIGHUP, starting generator")

	protocol := os.Getenv("OTLP_PROTOCOL")
	if protocol == "" {
		protocol = "grpc"
	}
	if protocol != "grpc" && protocol != "http" {
		log.Fatalf("unsupported OTLP_PROTOCOL %q: must be \"grpc\" or \"http\"", protocol)
	}
	log.Printf("using protocol: %s", protocol)

	mode := os.Getenv("GENERATOR_MODE")
	log.Printf("using generator mode: %q", mode)

	ctx := context.Background()

	endpoint := endpointGRPC
	if protocol == "http" {
		endpoint = endpointHTTP
	}

	if mode == "postgresqlreceiver" {
		res, err := resource.New(ctx,
			resource.WithAttributes(
				semconv.ServiceName("postgresql"),
				attribute.String("db.system", "postgresql"),
			),
		)
		if err != nil {
			log.Fatalf("failed to create resource: %v", err)
		}
		log.Printf("sending %d postgresql metrics to %s", count, endpoint)
		if err := sendPostgresMetrics(ctx, res, protocol); err != nil {
			log.Fatalf("postgresql metrics error: %v", err)
		}
		log.Println("postgresql metrics sent successfully")
		return
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(semconv.ServiceName("generator")),
	)
	if err != nil {
		log.Fatalf("failed to create resource: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()
		log.Printf("sending %d traces to %s", count, endpoint)
		if err := sendTraces(ctx, res, protocol); err != nil {
			log.Printf("traces error: %v", err)
			return
		}
		log.Println("traces sent successfully")
	}()

	go func() {
		defer wg.Done()
		log.Printf("sending %d metrics to %s", count, endpoint)
		if err := sendMetrics(ctx, res, protocol); err != nil {
			log.Printf("metrics error: %v", err)
			return
		}
		log.Println("metrics sent successfully")
	}()

	go func() {
		defer wg.Done()
		log.Printf("sending %d logs to %s", count, endpoint)
		if err := sendLogs(ctx, res, protocol); err != nil {
			log.Printf("logs error: %v", err)
			return
		}
		log.Println("logs sent successfully")
	}()

	// TODO: sendProfiles() — add when go.opentelemetry.io/otel/sdk/profiles stabilises.
	// wg.Add(1)
	// go func() {
	// 	defer wg.Done()
	// 	log.Printf("sending %d profiles to %s", count, endpoint)
	// 	if err := sendProfiles(ctx, res, protocol); err != nil {
	// 		log.Printf("profiles error: %v", err)
	// 		return
	// 	}
	// 	log.Println("profiles sent successfully")
	// }()

	wg.Wait()
	log.Println("all signals sent, exiting")
}

func sendTraces(ctx context.Context, res *resource.Resource, protocol string) error {
	var exporter sdktrace.SpanExporter
	var err error
	switch protocol {
	case "grpc":
		exporter, err = otlptracegrpc.New(ctx,
			otlptracegrpc.WithInsecure(),
			otlptracegrpc.WithEndpoint(endpointGRPC),
		)
	case "http":
		exporter, err = otlptracehttp.New(ctx,
			otlptracehttp.WithInsecure(),
			otlptracehttp.WithEndpoint(endpointHTTP),
		)
	}
	if err != nil {
		return fmt.Errorf("create trace exporter: %w", err)
	}
	log.Println("traces: exporter created")

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	defer tp.Shutdown(ctx)

	tracer := tp.Tracer("generator")
	for i := range count {
		_, span := tracer.Start(ctx, fmt.Sprintf("span-%d", i))
		span.AddEvent("generator.event", trace.WithAttributes(attribute.Int("event.index", i)))
		span.End()
	}
	log.Printf("traces: %d spans created, flushing", count)

	if err := tp.ForceFlush(ctx); err != nil {
		return fmt.Errorf("flush traces: %w", err)
	}
	return nil
}

func sendMetrics(ctx context.Context, res *resource.Resource, protocol string) error {
	var exporter sdkmetric.Exporter
	var err error
	switch protocol {
	case "grpc":
		exporter, err = otlpmetricgrpc.New(ctx,
			otlpmetricgrpc.WithInsecure(),
			otlpmetricgrpc.WithEndpoint(endpointGRPC),
		)
	case "http":
		exporter, err = otlpmetrichttp.New(ctx,
			otlpmetrichttp.WithInsecure(),
			otlpmetrichttp.WithEndpoint(endpointHTTP),
		)
	}
	if err != nil {
		return fmt.Errorf("create metric exporter: %w", err)
	}
	log.Println("metrics: exporter created")

	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exporter, sdkmetric.WithInterval(100*time.Millisecond))),
		sdkmetric.WithResource(res),
	)
	defer mp.Shutdown(ctx)

	meter := mp.Meter("generator")
	counter, err := meter.Int64Counter("generator.requests")
	if err != nil {
		return fmt.Errorf("create counter: %w", err)
	}

	gauge, err := meter.Float64Gauge("generator.value")
	if err != nil {
		return fmt.Errorf("create gauge: %w", err)
	}
	log.Println("metrics: instruments created")

	for i := range count {
		attrs := metric.WithAttributes(attribute.Int("index", i))
		counter.Add(ctx, 1, attrs)
		gauge.Record(ctx, float64(i), attrs)
	}
	log.Printf("metrics: %d data points recorded, flushing", count)

	if err := mp.ForceFlush(ctx); err != nil {
		return fmt.Errorf("flush metrics: %w", err)
	}
	return nil
}

func sendPostgresMetrics(ctx context.Context, res *resource.Resource, protocol string) error {
	var exporter sdkmetric.Exporter
	var err error
	switch protocol {
	case "grpc":
		exporter, err = otlpmetricgrpc.New(ctx,
			otlpmetricgrpc.WithInsecure(),
			otlpmetricgrpc.WithEndpoint(endpointGRPC),
		)
	case "http":
		exporter, err = otlpmetrichttp.New(ctx,
			otlpmetrichttp.WithInsecure(),
			otlpmetrichttp.WithEndpoint(endpointHTTP),
		)
	}
	if err != nil {
		return fmt.Errorf("create metric exporter: %w", err)
	}

	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exporter, sdkmetric.WithInterval(100*time.Millisecond))),
		sdkmetric.WithResource(res),
	)
	defer mp.Shutdown(ctx)

	meter := mp.Meter("postgresqlreceiver")

	rowsCounter, err := meter.Int64Counter("postgresql.rows",
		metric.WithDescription("The number of rows in the database."),
		metric.WithUnit("{row}"),
	)
	if err != nil {
		return fmt.Errorf("create postgresql.rows counter: %w", err)
	}

	commitsCounter, err := meter.Int64Counter("postgresql.commits",
		metric.WithDescription("The number of commits."),
		metric.WithUnit("{commit}"),
	)
	if err != nil {
		return fmt.Errorf("create postgresql.commits counter: %w", err)
	}

	dbSizeGauge, err := meter.Int64Gauge("postgresql.db_size",
		metric.WithDescription("The database disk usage."),
		metric.WithUnit("By"),
	)
	if err != nil {
		return fmt.Errorf("create postgresql.db_size gauge: %w", err)
	}

	blocksReadCounter, err := meter.Int64Counter("postgresql.blocks_read",
		metric.WithDescription("The number of blocks read."),
		metric.WithUnit("{block}"),
	)
	if err != nil {
		return fmt.Errorf("create postgresql.blocks_read counter: %w", err)
	}

	rollbacksCounter, err := meter.Int64Counter("postgresql.rollbacks",
		metric.WithDescription("The number of rollbacks."),
		metric.WithUnit("{rollback}"),
	)
	if err != nil {
		return fmt.Errorf("create postgresql.rollbacks counter: %w", err)
	}

	dbAttr := metric.WithAttributes(attribute.String("postgresql.database.name", "postgres"))

	for i := range count {
		rowsCounter.Add(ctx, int64(i+1), dbAttr,
			metric.WithAttributes(attribute.String("state", "live")))
		commitsCounter.Add(ctx, int64(i+1), dbAttr)
		dbSizeGauge.Record(ctx, int64((i+1)*1024), dbAttr)
		blocksReadCounter.Add(ctx, int64(i+1), dbAttr,
			metric.WithAttributes(attribute.String("source", "heap_read")))
		rollbacksCounter.Add(ctx, int64(i), dbAttr)
	}
	log.Printf("postgresql metrics: %d iterations recorded, flushing", count)

	if err := mp.ForceFlush(ctx); err != nil {
		return fmt.Errorf("flush postgresql metrics: %w", err)
	}
	return nil
}

func sendLogs(ctx context.Context, res *resource.Resource, protocol string) error {
	var exporter sdklog.Exporter
	var err error
	switch protocol {
	case "grpc":
		exporter, err = otlploggrpc.New(ctx,
			otlploggrpc.WithInsecure(),
			otlploggrpc.WithEndpoint(endpointGRPC),
		)
	case "http":
		exporter, err = otlploghttp.New(ctx,
			otlploghttp.WithInsecure(),
			otlploghttp.WithEndpoint(endpointHTTP),
		)
	}
	if err != nil {
		return fmt.Errorf("create log exporter: %w", err)
	}
	log.Println("logs: exporter created")

	lp := sdklog.NewLoggerProvider(
		sdklog.WithProcessor(sdklog.NewBatchProcessor(exporter)),
		sdklog.WithResource(res),
	)
	defer lp.Shutdown(ctx)

	logger := lp.Logger("generator")
	severities := []otellog.Severity{
		otellog.SeverityDebug,
		otellog.SeverityInfo,
		otellog.SeverityWarn,
		otellog.SeverityError,
	}
	for i := range count {
		var r otellog.Record
		r.SetSeverity(severities[i%len(severities)])
		r.SetBody(otellog.StringValue(fmt.Sprintf("log record %d", i)))
		r.SetTimestamp(time.Now())
		logger.Emit(ctx, r)
	}
	log.Printf("logs: %d records emitted, flushing", count)

	if err := lp.ForceFlush(ctx); err != nil {
		return fmt.Errorf("flush logs: %w", err)
	}
	return nil
}
