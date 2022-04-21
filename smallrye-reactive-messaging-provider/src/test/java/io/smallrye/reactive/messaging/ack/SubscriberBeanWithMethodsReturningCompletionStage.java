package io.smallrye.reactive.messaging.ack;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.reactivestreams.Publisher;

@ApplicationScoped
public class SubscriberBeanWithMethodsReturningCompletionStage extends SpiedBeanHelper {

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
    public CompletionStage<Void> subWithAck(Message<String> message) {
        return CompletableFuture.runAsync(() -> processed(MANUAL_ACKNOWLEDGMENT, message), executor)
                .thenCompose(x -> message.ack());
    }

    @Outgoing(MANUAL_ACKNOWLEDGMENT)
    public Publisher<Message<String>> sourceToManualAck() {
        return source(MANUAL_ACKNOWLEDGMENT);
    }

    @Incoming(NO_ACKNOWLEDGMENT_MESSAGE)
    @Acknowledgment(Acknowledgment.Strategy.NONE)
    public CompletionStage<Void> subWithNoAckMessage(Message<String> message) {
        processed(NO_ACKNOWLEDGMENT_MESSAGE, message);
        return CompletableFuture.runAsync(this::nap);
    }

    @Outgoing(NO_ACKNOWLEDGMENT_MESSAGE)
    public Publisher<Message<String>> sourceToNoAckMessage() {
        return source(NO_ACKNOWLEDGMENT_MESSAGE);
    }

    @Incoming(NO_ACKNOWLEDGMENT_PAYLOAD)
    @Acknowledgment(Acknowledgment.Strategy.NONE)
    public CompletionStage<Void> subWithNoAckPayload(Message<String> message) {
        processed(NO_ACKNOWLEDGMENT_PAYLOAD, message);
        return CompletableFuture.runAsync(this::nap);
    }

    @Outgoing(NO_ACKNOWLEDGMENT_PAYLOAD)
    public Publisher<Message<String>> sourceToNoAckPayload() {
        return source(NO_ACKNOWLEDGMENT_PAYLOAD);
    }

    @Incoming(PRE_PROCESSING_ACKNOWLEDGMENT_MESSAGE)
    @Acknowledgment(Acknowledgment.Strategy.PRE_PROCESSING)
    public CompletionStage<Void> preProcessingWithMessage(Message<String> message) {
        return CompletableFuture.runAsync(() -> processed(PRE_PROCESSING_ACKNOWLEDGMENT_MESSAGE, message), executor);
    }

    @Outgoing(PRE_PROCESSING_ACKNOWLEDGMENT_MESSAGE)
    public Publisher<Message<String>> sourceToPrePocessingMessage() {
        return source(PRE_PROCESSING_ACKNOWLEDGMENT_MESSAGE);
    }

    @Incoming(PRE_PROCESSING_ACKNOWLEDGMENT_PAYLOAD)
    @Acknowledgment(Acknowledgment.Strategy.PRE_PROCESSING)
    public CompletionStage<Void> preProcessingWithPayload(String payload) {
        return CompletableFuture.runAsync(() -> processed(PRE_PROCESSING_ACKNOWLEDGMENT_PAYLOAD, payload), executor);
    }

    @Outgoing(PRE_PROCESSING_ACKNOWLEDGMENT_PAYLOAD)
    public Publisher<Message<String>> sourceToPrePocessingPayload() {
        return source(PRE_PROCESSING_ACKNOWLEDGMENT_PAYLOAD);
    }

    @Incoming(POST_PROCESSING_ACKNOWLEDGMENT_MESSAGE)
    @Acknowledgment(Acknowledgment.Strategy.POST_PROCESSING)
    public CompletionStage<Void> postProcessingWithMessage(Message<String> message) {
        return CompletableFuture.runAsync(() -> processed(POST_PROCESSING_ACKNOWLEDGMENT_MESSAGE, message), executor);
    }

    @Outgoing(POST_PROCESSING_ACKNOWLEDGMENT_MESSAGE)
    public Publisher<Message<String>> sourceToPostPocessingMessage() {
        return source(POST_PROCESSING_ACKNOWLEDGMENT_MESSAGE);
    }

    @Incoming(POST_PROCESSING_ACKNOWLEDGMENT_PAYLOAD)
    @Acknowledgment(Acknowledgment.Strategy.POST_PROCESSING)
    public CompletionStage<Void> postProcessingWithPayload(String payload) {
        return CompletableFuture.runAsync(() -> processed(POST_PROCESSING_ACKNOWLEDGMENT_PAYLOAD, payload), executor);
    }

    @Outgoing(POST_PROCESSING_ACKNOWLEDGMENT_PAYLOAD)
    public Publisher<Message<String>> sourceToPostPocessingPayload() {
        return source(POST_PROCESSING_ACKNOWLEDGMENT_PAYLOAD);
    }

    @Incoming(DEFAULT_PROCESSING_ACKNOWLEDGMENT_MESSAGE)
    public CompletionStage<Void> defaultProcessingWithMessage(Message<String> message) {
        return CompletableFuture.runAsync(
                () -> processed(DEFAULT_PROCESSING_ACKNOWLEDGMENT_MESSAGE, message), executor)
                .thenCompose(x -> message.ack());
    }

    @Outgoing(DEFAULT_PROCESSING_ACKNOWLEDGMENT_MESSAGE)
    public Publisher<Message<String>> sourceToDefaultProcessingMessage() {
        return source(DEFAULT_PROCESSING_ACKNOWLEDGMENT_MESSAGE);
    }

    @Incoming(DEFAULT_PROCESSING_ACKNOWLEDGMENT_PAYLOAD)
    public CompletionStage<Void> defaultProcessingWithPayload(String payload) {
        return CompletableFuture
                .runAsync(() -> processed(DEFAULT_PROCESSING_ACKNOWLEDGMENT_PAYLOAD, payload), executor);
    }

    @Outgoing(DEFAULT_PROCESSING_ACKNOWLEDGMENT_PAYLOAD)
    public Publisher<Message<String>> defaultToPostProcessingPayload() {
        return source(DEFAULT_PROCESSING_ACKNOWLEDGMENT_PAYLOAD);
    }

}
