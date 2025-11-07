package io.smallrye.reactive.messaging.aws.sqs.tracing;

import io.opentelemetry.context.propagation.TextMapSetter;

public enum SqsTraceTextMapSetter implements TextMapSetter<SqsTrace> {
    INSTANCE;

    @Override
    public void set(final SqsTrace carrier, final String key, final String value) {
        if (carrier != null) {
            carrier.setProperty(key, value);
        }
    }
}
