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
    private final ReceivingMqttMessageMetadata receivingMetadata;

    ReceivingMqttMessage(MqttPublishMessage message, MqttFailureHandler onNack) {
        this.message = message;
        this.onNack = onNack;
        this.receivingMetadata = new ReceivingMqttMessageMetadata(this.message);
        this.metadata = captureContextMetadata(receivingMetadata);
    }

    @Override
    public byte[] getPayload() {
        return this.message.payload().getDelegate().getBytes();
    }

    @Override
    public Metadata getMetadata() {
        return metadata;
    }

    @Override
    public int getMessageId() {
        return receivingMetadata.getMessageId();
    }

    @Override
    public MqttQoS getQosLevel() {
        return receivingMetadata.getQosLevel();
    }

    @Override
    public boolean isDuplicate() {
        return receivingMetadata.isDuplicate();
    }

    @Override
    public boolean isRetain() {
        return receivingMetadata.isRetain();
    }

    @Override
    public String getTopic() {
        return receivingMetadata.getTopic();
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
