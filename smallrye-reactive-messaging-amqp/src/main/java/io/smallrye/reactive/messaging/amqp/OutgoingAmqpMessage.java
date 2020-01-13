package io.smallrye.reactive.messaging.amqp;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.vertx.core.json.JsonObject;

public class OutgoingAmqpMessage<T> extends AmqpMessage<T>
        implements org.eclipse.microprofile.reactive.messaging.Message<T> {

    private final Metadata metadata;

    public OutgoingAmqpMessage(io.vertx.axle.amqp.AmqpMessage message, Metadata metadata) {
        super(message);
        this.metadata = metadata;
    }

    @Override
    public Metadata getMetadata() {
        return metadata;
    }

    @Override
    public boolean isDurable() {
        return metadata.getAsBoolean(AmqpMetadata.OUTGOING_DURABLE);
    }

    @Override
    public int getPriority() {
        return metadata.getAsInteger(AmqpMetadata.OUTGOING_PRIORITY, 0);
    }

    @Override
    public long getTtl() {
        return metadata.getAsLong(AmqpMetadata.OUTGOING_TTL, 0);
    }

    @Override
    public Object getMessageId() {
        return metadata.get(AmqpMetadata.OUTGOING_ID);
    }

    @Override
    public String getAddress() {
        return metadata.getAsString(AmqpMetadata.OUTGOING_ADDRESS, null);
    }

    @Override
    public String getGroupId() {
        return metadata.getAsString(AmqpMetadata.OUTGOING_GROUP_ID, null);
    }

    @Override
    public String getContentType() {
        return metadata.getAsString(AmqpMetadata.OUTGOING_CONTENT_TYPE, null);
    }

    @Override
    public Object getCorrelationId() {
        return metadata.get(AmqpMetadata.OUTGOING_CORRELATION_ID);
    }

    @Override
    public String getContentEncoding() {
        return metadata.getAsString(AmqpMetadata.OUTGOING_CONTENT_ENCODING, null);
    }

    @Override
    public String getSubject() {
        return metadata.getAsString(AmqpMetadata.OUTGOING_SUBJECT, null);
    }

    @Override
    public CompletionStage<Void> ack() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public JsonObject getApplicationProperties() {
        return metadata.get(AmqpMetadata.OUTGOING_APPLICATION_PROPERTIES, new JsonObject());
    }
}
