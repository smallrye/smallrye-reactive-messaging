package io.smallrye.reactive.messaging.mqtt;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.netty.handler.codec.mqtt.MqttQoS;

public final class SendingMqttMessage<T> implements MqttMessage<T> {

    private final T payload;
    private final Supplier<CompletionStage<Void>> ack;
    private final SendingMqttMessageMetadata sendingMetadata;
    private final Metadata metadata;

    SendingMqttMessage(T payload, SendingMqttMessageMetadata metadata, Supplier<CompletionStage<Void>> ack) {
        this.payload = payload;
        this.ack = ack;
        this.sendingMetadata = metadata;
        this.metadata = Metadata.of(sendingMetadata);
    }

    SendingMqttMessage(T payload, SendingMqttMessageMetadata metadata) {
        this(payload, metadata, null);
    }

    @Override
    public Metadata getMetadata() {
        return metadata;
    }

    @Override
    public CompletionStage<Void> ack() {
        if (ack != null) {
            return ack.get();
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public Supplier<CompletionStage<Void>> getAck() {
        return this::ack;
    }

    @Override
    public T getPayload() {
        return payload;
    }

    @Override
    public int getMessageId() {
        return -1;
    }

    @Override
    public MqttQoS getQosLevel() {
        return sendingMetadata.getQosLevel();
    }

    @Override
    public boolean isDuplicate() {
        return false;
    }

    @Override
    public boolean isRetain() {
        return sendingMetadata.isRetain();
    }

    @Override
    public String getTopic() {
        return sendingMetadata.getTopic();
    }
}
