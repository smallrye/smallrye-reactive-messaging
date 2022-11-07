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

    SendingMqttMessage(String topic, T payload, MqttQoS qos, boolean isRetain, Supplier<CompletionStage<Void>> ack) {
        this.payload = payload;
        this.ack = ack;
        this.sendingMetadata = new SendingMqttMessageMetadata(topic, qos, isRetain);
        this.metadata = Metadata.of(sendingMetadata);
    }

    SendingMqttMessage(String topic, T payload, MqttQoS qos, boolean isRetain) {
        this(topic, payload, qos, isRetain, null);
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

    public T getPayload() {
        return payload;
    }

    public int getMessageId() {
        return -1;
    }

    public MqttQoS getQosLevel() {
        return sendingMetadata.getQosLevel();
    }

    public boolean isDuplicate() {
        return false;
    }

    public boolean isRetain() {
        return sendingMetadata.isRetain();
    }

    public String getTopic() {
        return sendingMetadata.getTopic();
    }
}
