package global

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	otrace "go.opentelemetry.io/otel/trace"
)

var heightTrace otrace.Tracer

func InitTracer(url string, serviceName string, environment string, id int64, jageer bool) error {
	if !jageer {
		return nil
	}
	defaultTracer, err := TracerProvider(url, serviceName, environment, id)
	if err != nil {
		return err
	}
	otel.SetTracerProvider(defaultTracer)
	return nil
}

func TracerProvider(url string, serviceName string, environment string, id int64) (*trace.TracerProvider, error) {
	// Create the Jaeger exporter
	opts, err := GetTracerProviderOptions(url, serviceName, environment, id)
	if err != nil {
		return nil, err
	}
	tp := trace.NewTracerProvider(opts...)
	return tp, nil
}

func GetTracerProviderOptions(url string, serviceName string, environment string, id int64) ([]trace.TracerProviderOption, error) {
	exp, err := jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(url)))
	if err != nil {
		return nil, err
	}
	return []trace.TracerProviderOption{
		// Always be sure to batch in production.
		trace.WithBatcher(exp),
		// Record information about this application in a Resource.
		trace.WithResource(resource.NewWithAttributes(
			"https://opentelemetry.io/schemas/1.9.0",
			attribute.Key("service.name").String(serviceName),
			attribute.String("environment", environment),
			attribute.Int64("ID", id),
		)),
	}, nil
}

func GetNewHeightTracer() otrace.Tracer {
	tr := otel.GetTracerProvider()
	if tr != nil {
		heightTrace = tr.Tracer("NewHeight")
		return heightTrace
	}
	return nil
}

func GetHeightTrace() otrace.Tracer {
	if heightTrace != nil {
		return heightTrace
	} else {
		return GetNewHeightTracer()
	}
}

func GetNewTrace(name string) otrace.Tracer {
	tr := otel.GetTracerProvider()
	if tr != nil {
		return tr.Tracer(name)
	}
	return nil
}
