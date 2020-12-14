package io.smallrye.reactive.messaging.mqtt.server;

import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.mutiny.mqtt.messages.MqttPublishMessage;

public class MqttMessage implements Message<byte[]> {

    private final MqttPublishMessage message;
    private final String clientId;
    private final Supplier<CompletionStage<Void>> ack;

    MqttMessage(MqttPublishMessage message, String clientId,
            Supplier<CompletionStage<Void>> ack) {
        this.message = message;
        this.clientId = clientId;
        this.ack = ack;
    }

    @Override
    public byte[] getPayload() {
        return this.message.payload().getDelegate().getBytes();
    }

    @Override
    public CompletionStage<Void> ack() {
        return ack.get();
    }

    @Override
    public Supplier<CompletionStage<Void>> getAck() {
        return this::ack;
    }

    public int getMessageId() {
        return message.messageId();
    }

    public MqttQoS getQosLevel() {
        return message.qosLevel();
    }

    public boolean isDuplicate() {
        return message.isDup();
    }

    public boolean isRetain() {
        return message.isRetain();
    }

    public String getTopic() {
        return message.topicName();
    }

    public String getClientId() {
        return clientId;
    }
}
