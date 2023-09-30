package io.smallrye.reactive.messaging.aws.sqs.tracing;

import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.context.Context;
import io.opentelemetry.instrumentation.api.instrumenter.AttributesExtractor;

public enum SqsAttributesExtractor implements AttributesExtractor<SqsTrace, Void> {
    INSTANCE;

    @Override
    public void onStart(final AttributesBuilder attributes, final Context parentContext, final SqsTrace request) {

    }

    @Override
    public void onEnd(
            final AttributesBuilder attributes,
            final Context context,
            final SqsTrace request,
            final Void response,
            final Throwable error) {

    }
}
