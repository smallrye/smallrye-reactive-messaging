package io.smallrye.reactive.messaging.aws.sqs.tracing;

import io.opentelemetry.context.propagation.TextMapSetter;

public enum SqsTraceTextMapSetter implements TextMapSetter<SqsTrace> {
    INSTANCE;

    @Override
    public void set(SqsTrace carrier, String key, String value) {
        if (carrier != null) {

        }
    }
}
