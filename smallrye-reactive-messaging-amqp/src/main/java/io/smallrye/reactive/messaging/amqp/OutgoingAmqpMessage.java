package io.smallrye.reactive.messaging.amqp;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.eclipse.microprofile.reactive.messaging.Headers;

import io.vertx.core.json.JsonObject;

public class OutgoingAmqpMessage<T> extends AmqpMessage<T>
        implements org.eclipse.microprofile.reactive.messaging.Message<T> {

    private final Headers headers;

    public OutgoingAmqpMessage(io.vertx.axle.amqp.AmqpMessage message, Headers headers) {
        super(message);
        this.headers = headers;
    }

    @Override
    public Headers getHeaders() {
        return headers;
    }

    @Override
    public boolean isDurable() {
        return headers.getAsBoolean(AmqpHeaders.OUTGOING_DURABLE);
    }

    @Override
    public int getPriority() {
        return headers.getAsInteger(AmqpHeaders.OUTGOING_PRIORITY, 0);
    }

    @Override
    public long getTtl() {
        return headers.getAsLong(AmqpHeaders.OUTGOING_TTL, 0);
    }

    @Override
    public Object getMessageId() {
        return headers.get(AmqpHeaders.OUTGOING_ID);
    }

    @Override
    public String getAddress() {
        return headers.getAsString(AmqpHeaders.OUTGOING_ADDRESS, null);
    }

    @Override
    public String getGroupId() {
        return headers.getAsString(AmqpHeaders.OUTGOING_GROUP_ID, null);
    }

    @Override
    public String getContentType() {
        return headers.getAsString(AmqpHeaders.OUTGOING_CONTENT_TYPE, null);
    }

    @Override
    public Object getCorrelationId() {
        return headers.get(AmqpHeaders.OUTGOING_CORRELATION_ID);
    }

    @Override
    public String getContentEncoding() {
        return headers.getAsString(AmqpHeaders.OUTGOING_CONTENT_ENCODING, null);
    }

    @Override
    public String getSubject() {
        return headers.getAsString(AmqpHeaders.OUTGOING_SUBJECT, null);
    }

    @Override
    public CompletionStage<Void> ack() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public JsonObject getApplicationProperties() {
        return headers.get(AmqpHeaders.OUTGOING_APPLICATION_PROPERTIES, new JsonObject());
    }
}
