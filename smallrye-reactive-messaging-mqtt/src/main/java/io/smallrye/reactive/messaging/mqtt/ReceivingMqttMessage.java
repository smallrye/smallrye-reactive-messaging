package io.smallrye.reactive.messaging.mqtt;

import static io.smallrye.reactive.messaging.providers.locals.ContextAwareMessage.captureContextMetadata;

import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.mutiny.mqtt.messages.MqttPublishMessage;

public class ReceivingMqttMessage implements MqttMessage<byte[]> {
    final MqttPublishMessage message;
    final MqttFailureHandler onNack;
    final Metadata metadata;

    ReceivingMqttMessage(MqttPublishMessage message, MqttFailureHandler onNack) {
        this.message = message;
        this.onNack = onNack;
        this.metadata = captureContextMetadata();
    }

    @Override
    public byte[] getPayload() {
        return this.message.payload().getDelegate().getBytes();
    }

    @Override
    public Metadata getMetadata() {
        return metadata;
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

    @Override
    public CompletionStage<Void> nack(Throwable reason, Metadata metadata) {
        return this.onNack.handle(reason);
    }

    @Override
    public Function<Throwable, CompletionStage<Void>> getNack() {
        return this::nack;
    }
}
