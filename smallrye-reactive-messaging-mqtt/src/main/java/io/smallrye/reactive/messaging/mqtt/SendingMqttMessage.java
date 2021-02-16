package io.smallrye.reactive.messaging.mqtt;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import com.hivemq.client.internal.shaded.org.jetbrains.annotations.NotNull;

import io.netty.handler.codec.mqtt.MqttQoS;

public final class SendingMqttMessage<T> implements MqttMessage<T> {

    private final String topic;
    private final T payload;
    private final MqttQoS qos;
    private final boolean isRetain;
    private final Supplier<CompletionStage<Void>> ack;

    SendingMqttMessage(String topic, T payload, MqttQoS qos, boolean isRetain, Supplier<CompletionStage<Void>> ack) {
        this.topic = topic;
        this.payload = payload;
        this.qos = qos;
        this.isRetain = isRetain;
        this.ack = ack;
    }

    SendingMqttMessage(String topic, T payload, MqttQoS qos, boolean isRetain) {
        this(topic, payload, qos, isRetain, null);
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

    public @NotNull MqttQoS getQosLevel() {
        return qos;
    }

    public boolean isDuplicate() {
        return false;
    }

    public boolean isRetain() {
        return isRetain;
    }

    public String getTopic() {
        return topic;
    }
}
