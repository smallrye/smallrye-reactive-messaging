package io.smallrye.reactive.messaging.mqtt.hivemq;

import com.hivemq.client.mqtt.mqtt3.message.publish.Mqtt3Publish;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.smallrye.reactive.messaging.mqtt.MqttFailureHandler;
import io.smallrye.reactive.messaging.mqtt.MqttMessage;

import java.util.concurrent.CompletionStage;
import java.util.function.Function;

public class ReceivingMqttMessage implements MqttMessage<byte[]> {
    final Mqtt3Publish message;
    final MqttFailureHandler onNack;

    ReceivingMqttMessage(Mqtt3Publish message, MqttFailureHandler onNack) {
        this.message = message;
        this.onNack = onNack;
    }

    @Override
    public byte[] getPayload() {
        return this.message.getPayloadAsBytes();
    }

    public int getMessageId() {
        return -1;
    }

    public MqttQoS getQosLevel() {
        return MqttQoS.valueOf(message.getQos().getCode());
    }

    public boolean isDuplicate() {
        return false;
    }

    public boolean isRetain() {
        return message.isRetain();
    }

    public String getTopic() {
        return message.getTopic().toString();
    }

    @Override
    public CompletionStage<Void> nack(Throwable reason) {
        return this.onNack.handle(reason);
    }

    @Override
    public Function<Throwable, CompletionStage<Void>> getNack() {
        return this::nack;
    }
}
