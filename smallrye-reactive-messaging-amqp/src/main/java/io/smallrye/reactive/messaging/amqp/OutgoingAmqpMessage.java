package io.smallrye.reactive.messaging.amqp;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.Supplier;

import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.vertx.core.json.JsonObject;

public class OutgoingAmqpMessage<T> extends AmqpMessage<T>
        implements org.eclipse.microprofile.reactive.messaging.Message<T> {

    private final Metadata metadata;
    private final OutgoingAmqpMetadata amqpMetadata;

    public OutgoingAmqpMessage(io.vertx.mutiny.amqp.AmqpMessage message, OutgoingAmqpMetadata amqpMetadata) {
        super(message, null, null, false, false);
        this.amqpMetadata = amqpMetadata;
        this.metadata = Metadata.of(amqpMetadata);
    }

    @Override
    public Metadata getMetadata() {
        return metadata;
    }

    @Override
    public boolean isDurable() {
        return amqpMetadata.isDurable();
    }

    @Override
    public int getPriority() {
        return amqpMetadata.getPriority();
    }

    @Override
    public long getTtl() {
        return amqpMetadata.getTtl();
    }

    @Override
    public Object getMessageId() {
        return amqpMetadata.getMessageId();
    }

    @Override
    public String getAddress() {
        return amqpMetadata.getAddress();
    }

    @Override
    public String getGroupId() {
        return amqpMetadata.getGroupId();
    }

    @Override
    public String getContentType() {
        return amqpMetadata.getContentType();
    }

    @Override
    public Object getCorrelationId() {
        return amqpMetadata.getCorrelationId();
    }

    @Override
    public String getContentEncoding() {
        return amqpMetadata.getContentEncoding();
    }

    @Override
    public String getSubject() {
        return amqpMetadata.getSubject();
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
        return amqpMetadata.getProperties();
    }
}
