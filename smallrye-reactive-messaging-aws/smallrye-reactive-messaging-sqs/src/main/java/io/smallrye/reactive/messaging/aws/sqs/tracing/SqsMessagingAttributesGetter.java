package io.smallrye.reactive.messaging.aws.sqs.tracing;

import io.opentelemetry.instrumentation.api.instrumenter.messaging.MessagingAttributesGetter;
import io.opentelemetry.instrumentation.api.instrumenter.network.NetworkAttributesGetter;

public enum SqsMessagingAttributesGetter implements MessagingAttributesGetter<SqsTrace, Void>,
        NetworkAttributesGetter<SqsTrace, Void> {
    INSTANCE;

    @Override
    public String getSystem(SqsTrace request) {
        return "AmazonSQS";
    }

    @Override
    public String getDestination(SqsTrace request) {
        return request.getQueue();
    }

    @Override
    public String getNetworkProtocolName(SqsTrace request, Void response) {
        return "sqs";
    }

    @Override
    public String getNetworkProtocolVersion(SqsTrace request, Void response) {
        return "2012-11-05";
    }

    @Override
    public boolean isTemporaryDestination(SqsTrace request) {
        return false;
    }

    @Override
    public String getConversationId(SqsTrace request) {
        return request.getConversationId();
    }

    @Override
    public Long getMessagePayloadSize(SqsTrace request) {
        return request.getMessagePayloadSize();
    }

    @Override
    public Long getMessagePayloadCompressedSize(SqsTrace request) {
        return null;
    }

    @Override
    public String getMessageId(SqsTrace request, Void response) {
        return request.getMessageId();
    }
}
