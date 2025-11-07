package io.smallrye.reactive.messaging.aws.sqs.tracing;

import io.opentelemetry.context.propagation.TextMapGetter;

public enum SqsTraceTextMapGetter implements TextMapGetter<SqsTrace> {
    INSTANCE;

    @Override
    public Iterable<String> keys(final SqsTrace carrier) {
        return carrier.getPropertyNames();
    }

    @Override
    public String get(final SqsTrace carrier, final String key) {
        if (carrier != null) {
            return carrier.getProperty(key);
        }
        return null;
    }
}
