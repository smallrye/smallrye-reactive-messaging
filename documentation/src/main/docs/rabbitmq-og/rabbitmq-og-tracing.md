# OpenTelemetry Tracing

The RabbitMQ OG connector supports [OpenTelemetry](https://opentelemetry.io/)
tracing for both incoming and outgoing channels.

## Enabling tracing

Tracing is enabled by default. You can disable it per channel with:

```properties
mp.messaging.incoming.prices.tracing.enabled=false
mp.messaging.outgoing.prices.tracing.enabled=false
```

## How it works

When tracing is enabled:

-   **Outgoing messages**: The connector creates a `PUBLISH` span and
    injects the trace context into the message headers before sending.

-   **Incoming messages**: The connector extracts the trace context from
    the message headers and creates a `RECEIVE` span linked to the
    producer's trace context.

The span attributes include the exchange name and routing key.

## Including headers as span attributes

You can configure specific message headers to be recorded as span
attributes using the `tracing.attribute-headers` property:

```properties
mp.messaging.incoming.prices.tracing.attribute-headers=my-header,another-header
```

This is a comma-separated list of header names whose values will be
added as attributes to the tracing span.
