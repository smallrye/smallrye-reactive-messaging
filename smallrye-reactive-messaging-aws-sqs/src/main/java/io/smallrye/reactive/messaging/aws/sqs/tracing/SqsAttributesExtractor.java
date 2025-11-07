package io.smallrye.reactive.messaging.aws.sqs.tracing;

import java.util.Collections;
import java.util.List;

import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.context.Context;
import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessagingAttributesGetter;
import io.opentelemetry.instrumentation.api.instrumenter.AttributesExtractor;

public class SqsAttributesExtractor implements AttributesExtractor<SqsTrace, Void> {
    private final MessagingAttributesGetter<SqsTrace, Void> messagingAttributesGetter;

    public SqsAttributesExtractor() {
        this.messagingAttributesGetter = new SqsMessagingAttributesGetter();
    }

    @Override
    public void onStart(final AttributesBuilder attributes, final Context parentContext, final SqsTrace SqsTrace) {

    }

    @Override
    public void onEnd(
            final AttributesBuilder attributes,
            final Context context,
            final SqsTrace SqsTrace,
            final Void unused,
            final Throwable error) {

    }

    public MessagingAttributesGetter<SqsTrace, Void> getMessagingAttributesGetter() {
        return messagingAttributesGetter;
    }

    private static final class SqsMessagingAttributesGetter implements MessagingAttributesGetter<SqsTrace, Void> {
        @Override
        public String getSystem(final SqsTrace sqsTrace) {
            return "sqs";
        }

        @Override
        public String getDestination(final SqsTrace sqsTrace) {
            return sqsTrace.getQueue();
        }

        @Override
        public boolean isTemporaryDestination(final SqsTrace SqsTrace) {
            return false;
        }

        @Override
        public String getConversationId(final SqsTrace SqsTrace) {
            return null;
        }

        @Override
        public String getMessageId(final SqsTrace sqsTrace, final Void unused) {
            return sqsTrace.getMessageId();
        }

        @Override
        public List<String> getMessageHeader(SqsTrace sqsTrace, String name) {
            return Collections.emptyList();
        }

        @Override
        public String getDestinationTemplate(SqsTrace sqsTrace) {
            return null;
        }

        @Override
        public boolean isAnonymousDestination(SqsTrace sqsTrace) {
            return false;
        }

        @Override
        public Long getMessageBodySize(SqsTrace sqsTrace) {
            return null;
        }

        @Override
        public Long getMessageEnvelopeSize(SqsTrace sqsTrace) {
            return null;
        }

        @Override
        public String getClientId(SqsTrace sqsTrace) {
            return null;
        }

        @Override
        public Long getBatchMessageCount(SqsTrace sqsTrace, Void unused) {
            return null;
        }
    }
}
