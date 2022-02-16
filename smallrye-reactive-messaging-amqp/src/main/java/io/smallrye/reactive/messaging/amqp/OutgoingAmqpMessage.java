package io.smallrye.reactive.messaging.amqp;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.Supplier;

import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.vertx.core.json.JsonObject;

public class OutgoingAmqpMessage<T> extends AmqpMessage<T>
        implements org.eclipse.microprofile.reactive.messaging.Message<T> {

    private final OutgoingAmqpMetadata outgoingAmqpMetadata;

    public OutgoingAmqpMessage(io.vertx.mutiny.amqp.AmqpMessage message, OutgoingAmqpMetadata amqpMetadata) {
        super(message, null, amqpMetadata);
        this.outgoingAmqpMetadata = amqpMetadata;
    }

    @Override
    public Metadata getMetadata() {
        return metadata;
    }

    @Override
    public boolean isDurable() {
        return outgoingAmqpMetadata.isDurable();
    }

    @Override
    public int getPriority() {
        return outgoingAmqpMetadata.getPriority();
    }

    @Override
    public long getTtl() {
        return outgoingAmqpMetadata.getTtl();
    }

    @Override
    public Object getMessageId() {
        return outgoingAmqpMetadata.getMessageId();
    }

    @Override
    public String getAddress() {
        return outgoingAmqpMetadata.getAddress();
    }

    @Override
    public String getGroupId() {
        return outgoingAmqpMetadata.getGroupId();
    }

    @Override
    public String getContentType() {
        return outgoingAmqpMetadata.getContentType();
    }

    @Override
    public Object getCorrelationId() {
        return outgoingAmqpMetadata.getCorrelationId();
    }

    @Override
    public String getContentEncoding() {
        return outgoingAmqpMetadata.getContentEncoding();
    }

    @Override
    public long getExpiryTime() {
        return outgoingAmqpMetadata.getExpiryTime();
    }

    @Override
    public long getCreationTime() {
        return outgoingAmqpMetadata.getCreationTime();
    }

    @Override
    public long getDeliveryCount() {
        return 0;
    }

    @Override
    public long getGroupSequence() {
        return outgoingAmqpMetadata.getGroupSequence();
    }

    @Override
    public String getSubject() {
        return outgoingAmqpMetadata.getSubject();
    }

    @Override
    public CompletionStage<Void> ack() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<Void> nack(Throwable reason, Metadata metadata) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public Supplier<CompletionStage<Void>> getAck() {
        return this::ack;
    }

    @Override
    public Function<Throwable, CompletionStage<Void>> getNack() {
        return this::nack;
    }

    @Override
    public JsonObject getApplicationProperties() {
        return outgoingAmqpMetadata.getProperties();
    }
}
