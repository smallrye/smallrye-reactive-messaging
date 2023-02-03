package io.smallrye.reactive.messaging.ack;

import java.time.Duration;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.reactivestreams.Publisher;

import io.smallrye.mutiny.Uni;

@ApplicationScoped
public class SubscriberBeanWithMethodsReturningUni extends SpiedBeanHelper {

    public static final String MANUAL_ACKNOWLEDGMENT = "manual-acknowledgment";

    public static final String NO_ACKNOWLEDGMENT_MESSAGE = "no-acknowledgment-message";
    public static final String NO_ACKNOWLEDGMENT_PAYLOAD = "no-acknowledgment-payload";

    public static final String PRE_PROCESSING_ACKNOWLEDGMENT_MESSAGE = "pre-processing-acknowledgment-message";
    public static final String PRE_PROCESSING_ACKNOWLEDGMENT_PAYLOAD = "pre-processing-acknowledgment-payload";

    public static final String POST_PROCESSING_ACKNOWLEDGMENT_MESSAGE = "post-processing-acknowledgment-message";
    public static final String POST_PROCESSING_ACKNOWLEDGMENT_PAYLOAD = "post-processing-acknowledgment-payload";

    public static final String DEFAULT_PROCESSING_ACKNOWLEDGMENT_MESSAGE = "default-acknowledgment-message";
    public static final String DEFAULT_PROCESSING_ACKNOWLEDGMENT_PAYLOAD = "default-acknowledgment-payload";

    @Incoming(MANUAL_ACKNOWLEDGMENT)
    @Acknowledgment(Acknowledgment.Strategy.MANUAL)
    public Uni<Void> subWithAck(Message<String> message) {
        return Uni.createFrom().item(message)
                .emitOn(executor)
                .onItem().invoke((m) -> processed(MANUAL_ACKNOWLEDGMENT, message))
                .onItem().transformToUni(m -> Uni.createFrom().completionStage(m.ack()));
    }

    @Outgoing(MANUAL_ACKNOWLEDGMENT)
    public Publisher<Message<String>> sourceToManualAck() {
        return source(MANUAL_ACKNOWLEDGMENT);
    }

    @Incoming(NO_ACKNOWLEDGMENT_MESSAGE)
    @Acknowledgment(Acknowledgment.Strategy.NONE)
    public Uni<Void> subWithNoAckMessage(Message<String> message) {
        return Uni.createFrom().item(message)
                .emitOn(executor)
                .onItem().invoke((m) -> processed(NO_ACKNOWLEDGMENT_MESSAGE, message))
                .onItem().delayIt().by(Duration.ofMillis(10))
                .onItem().ignore().andContinueWithNull();
    }

    @Outgoing(NO_ACKNOWLEDGMENT_MESSAGE)
    public Publisher<Message<String>> sourceToNoAckMessage() {
        return source(NO_ACKNOWLEDGMENT_MESSAGE);
    }

    @Incoming(NO_ACKNOWLEDGMENT_PAYLOAD)
    @Acknowledgment(Acknowledgment.Strategy.NONE)
    public Uni<Void> subWithNoAckPayload(Message<String> message) {
        processed(NO_ACKNOWLEDGMENT_PAYLOAD, message);
        return Uni.createFrom().<Void> item(() -> null)
                .onItem().delayIt().by(Duration.ofMillis(10));
    }

    @Outgoing(NO_ACKNOWLEDGMENT_PAYLOAD)
    public Publisher<Message<String>> sourceToNoAckPayload() {
        return source(NO_ACKNOWLEDGMENT_PAYLOAD);
    }

    @Incoming(PRE_PROCESSING_ACKNOWLEDGMENT_MESSAGE)
    @Acknowledgment(Acknowledgment.Strategy.PRE_PROCESSING)
    public Uni<Void> preProcessingWithMessage(Message<String> message) {
        return Uni.createFrom().<Void> item(() -> null)
                .emitOn(executor)
                .onItem().invoke(x -> processed(PRE_PROCESSING_ACKNOWLEDGMENT_MESSAGE, message));
    }

    @Outgoing(PRE_PROCESSING_ACKNOWLEDGMENT_MESSAGE)
    public Publisher<Message<String>> sourceToPreProcessingMessage() {
        return source(PRE_PROCESSING_ACKNOWLEDGMENT_MESSAGE);
    }

    @Incoming(PRE_PROCESSING_ACKNOWLEDGMENT_PAYLOAD)
    @Acknowledgment(Acknowledgment.Strategy.PRE_PROCESSING)
    public Uni<Void> preProcessingWithPayload(String payload) {
        return Uni.createFrom().<Void> item(() -> null)
                .emitOn(executor)
                .onItem().invoke(x -> processed(PRE_PROCESSING_ACKNOWLEDGMENT_PAYLOAD, payload));
    }

    @Outgoing(PRE_PROCESSING_ACKNOWLEDGMENT_PAYLOAD)
    public Publisher<Message<String>> sourceToPrePocessingPayload() {
        return source(PRE_PROCESSING_ACKNOWLEDGMENT_PAYLOAD);
    }

    @Incoming(POST_PROCESSING_ACKNOWLEDGMENT_MESSAGE)
    @Acknowledgment(Acknowledgment.Strategy.POST_PROCESSING)
    public Uni<Void> postProcessingWithMessage(Message<String> message) {
        return Uni.createFrom().<Void> item(() -> null)
                .emitOn(executor)
                .onItem().invoke(x -> processed(POST_PROCESSING_ACKNOWLEDGMENT_MESSAGE, message));
    }

    @Outgoing(POST_PROCESSING_ACKNOWLEDGMENT_MESSAGE)
    public Publisher<Message<String>> sourceToPostPocessingMessage() {
        return source(POST_PROCESSING_ACKNOWLEDGMENT_MESSAGE);
    }

    @Incoming(POST_PROCESSING_ACKNOWLEDGMENT_PAYLOAD)
    @Acknowledgment(Acknowledgment.Strategy.POST_PROCESSING)
    public Uni<Void> postProcessingWithPayload(String payload) {
        return Uni.createFrom().<Void> item(() -> null)
                .emitOn(executor)
                .onItem().invoke(x -> processed(POST_PROCESSING_ACKNOWLEDGMENT_PAYLOAD, payload));
    }

    @Outgoing(POST_PROCESSING_ACKNOWLEDGMENT_PAYLOAD)
    public Publisher<Message<String>> sourceToPostProcessingPayload() {
        return source(POST_PROCESSING_ACKNOWLEDGMENT_PAYLOAD);
    }

    @Incoming(DEFAULT_PROCESSING_ACKNOWLEDGMENT_MESSAGE)
    public Uni<Void> defaultProcessingWithMessage(Message<String> message) {
        return Uni.createFrom().<Void> item(() -> null)
                .emitOn(executor)
                .onItem().invoke(x -> processed(DEFAULT_PROCESSING_ACKNOWLEDGMENT_MESSAGE, message))
                .onItem().transformToUni(x -> Uni.createFrom().completionStage(message.ack()));
    }

    @Outgoing(DEFAULT_PROCESSING_ACKNOWLEDGMENT_MESSAGE)
    public Publisher<Message<String>> sourceToDefaultProcessingMessage() {
        return source(DEFAULT_PROCESSING_ACKNOWLEDGMENT_MESSAGE);
    }

    @Incoming(DEFAULT_PROCESSING_ACKNOWLEDGMENT_PAYLOAD)
    public Uni<Void> defaultProcessingWithPayload(String payload) {
        return Uni.createFrom().<Void> item(() -> null)
                .emitOn(executor)
                .onItem().invoke(x -> processed(DEFAULT_PROCESSING_ACKNOWLEDGMENT_PAYLOAD, payload));
    }

    @Outgoing(DEFAULT_PROCESSING_ACKNOWLEDGMENT_PAYLOAD)
    public Publisher<Message<String>> defaultToPostProcessingPayload() {
        return source(DEFAULT_PROCESSING_ACKNOWLEDGMENT_PAYLOAD);
    }

}
