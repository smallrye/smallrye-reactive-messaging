package io.smallrye.reactive.messaging.ack;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.reactivestreams.Publisher;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.annotations.Merge;

@ApplicationScoped
public class BeanWithProcessorsManipulatingMessages extends SpiedBeanHelper {

    static final String MANUAL_ACKNOWLEDGMENT_CS = "manual-acknowledgment-cs";
    static final String MANUAL_ACKNOWLEDGMENT_UNI = "manual-acknowledgment-uni";

    static final String NO_ACKNOWLEDGMENT = "no-acknowledgment";
    static final String NO_ACKNOWLEDGMENT_CS = "no-acknowledgment-cs";
    static final String NO_ACKNOWLEDGMENT_UNI = "no-acknowledgment-uni";

    static final String PRE_ACKNOWLEDGMENT = "pre-acknowledgment";
    static final String PRE_ACKNOWLEDGMENT_CS = "pre-acknowledgment-cs";
    static final String PRE_ACKNOWLEDGMENT_UNI = "pre-acknowledgment-uni";

    static final String DEFAULT_ACKNOWLEDGMENT = "default-acknowledgment";
    static final String DEFAULT_ACKNOWLEDGMENT_CS = "default-acknowledgment-cs";
    static final String DEFAULT_ACKNOWLEDGMENT_UNI = "default-acknowledgment-uni";

    @Incoming("sink")
    @Merge
    @Acknowledgment(Acknowledgment.Strategy.MANUAL)
    public CompletionStage<Void> justTheSink(Message<String> in) {
        return in.ack();
    }

    @Incoming(MANUAL_ACKNOWLEDGMENT_CS)
    @Acknowledgment(Acknowledgment.Strategy.MANUAL)
    @Outgoing("sink")
    public CompletionStage<Message<String>> processorWithAck(Message<String> input) {
        processed(MANUAL_ACKNOWLEDGMENT_CS, input);
        return input.ack().thenApply(x -> Message.of(input.getPayload() + "1"));
    }

    @Outgoing(MANUAL_ACKNOWLEDGMENT_CS)
    public Publisher<Message<String>> sourceToManualAck() {
        return ReactiveStreams.of("a", "b", "c", "d", "e")
                .map(payload -> Message.of(payload, () -> CompletableFuture.runAsync(() -> {
                    nap();
                    acknowledged(MANUAL_ACKNOWLEDGMENT_CS, payload);
                }))).buildRs();
    }

    @Incoming(MANUAL_ACKNOWLEDGMENT_UNI)
    @Acknowledgment(Acknowledgment.Strategy.MANUAL)
    @Outgoing("sink")
    public Uni<Message<String>> processorWithAckUni(Message<String> input) {
        processed(MANUAL_ACKNOWLEDGMENT_UNI, input);
        return Uni.createFrom().completionStage(() -> input.ack()
                .thenApply(x -> Message.of(input.getPayload() + "1")));
    }

    @Outgoing(MANUAL_ACKNOWLEDGMENT_UNI)
    public Publisher<Message<String>> sourceToManualAckUni() {
        return ReactiveStreams.of("a", "b", "c", "d", "e")
                .map(payload -> Message.of(payload, () -> CompletableFuture.runAsync(() -> {
                    nap();
                    acknowledged(MANUAL_ACKNOWLEDGMENT_UNI, payload);
                }))).buildRs();
    }

    @Incoming(NO_ACKNOWLEDGMENT)
    @Acknowledgment(Acknowledgment.Strategy.NONE)
    @Outgoing("sink")
    public Message<String> processorWithNoAck(Message<String> input) {
        processed(NO_ACKNOWLEDGMENT, input);
        return Message.of(input.getPayload() + "1");
    }

    @Outgoing(NO_ACKNOWLEDGMENT)
    public Publisher<Message<String>> sourceToNoAck() {
        return Multi.createFrom().items("a", "b", "c", "d", "e")
                .map(payload -> Message.of(payload, () -> {
                    nap();
                    acknowledged(NO_ACKNOWLEDGMENT, payload);
                    return CompletableFuture.completedFuture(null);
                }));
    }

    @Incoming(NO_ACKNOWLEDGMENT_CS)
    @Acknowledgment(Acknowledgment.Strategy.NONE)
    @Outgoing("sink")
    public CompletionStage<Message<String>> processorWithNoAckCS(Message<String> input) {
        return CompletableFuture.completedFuture(input)
                .thenApply(m -> {
                    processed(NO_ACKNOWLEDGMENT_CS, input);
                    return Message.of(m.getPayload() + "1");
                });
    }

    @Outgoing(NO_ACKNOWLEDGMENT_CS)
    public Publisher<Message<String>> sourceToNoAckCS() {
        return Multi.createFrom().items("a", "b", "c", "d", "e")
                .map(payload -> Message.of(payload, () -> {
                    nap();
                    acknowledged(NO_ACKNOWLEDGMENT_CS, payload);
                    return CompletableFuture.completedFuture(null);
                }));
    }

    @Incoming(NO_ACKNOWLEDGMENT_UNI)
    @Acknowledgment(Acknowledgment.Strategy.NONE)
    @Outgoing("sink")
    public Uni<Message<String>> processorWithNoAckUni(Message<String> input) {
        return Uni.createFrom().completionStage(() -> CompletableFuture.completedFuture(input)
                .thenApply(m -> {
                    processed(NO_ACKNOWLEDGMENT_UNI, input);
                    return Message.of(m.getPayload() + "1");
                }));
    }

    @Outgoing(NO_ACKNOWLEDGMENT_UNI)
    public Publisher<Message<String>> sourceToNoAckUni() {
        return Multi.createFrom().items("a", "b", "c", "d", "e")
                .map(payload -> Message.of(payload, () -> {
                    nap();
                    acknowledged(NO_ACKNOWLEDGMENT_UNI, payload);
                    return CompletableFuture.completedFuture(null);
                }));
    }

    @Incoming(PRE_ACKNOWLEDGMENT)
    @Acknowledgment(Acknowledgment.Strategy.PRE_PROCESSING)
    @Outgoing("sink")
    public Message<String> processorWithPreAck(Message<String> input) {
        processed(PRE_ACKNOWLEDGMENT, input);
        return Message.of(input.getPayload() + "1");
    }

    @Outgoing(PRE_ACKNOWLEDGMENT)
    public Publisher<Message<String>> sourceToPreAck() {
        return Multi.createFrom().items("a", "b", "c", "d", "e")
                .map(payload -> Message.of(payload, () -> {
                    nap();
                    acknowledged(PRE_ACKNOWLEDGMENT, payload);
                    return CompletableFuture.completedFuture(null);
                }));
    }

    @Incoming(PRE_ACKNOWLEDGMENT_CS)
    @Acknowledgment(Acknowledgment.Strategy.PRE_PROCESSING)
    @Outgoing("sink")
    public CompletionStage<Message<String>> processorWithPreAckCS(Message<String> input) {
        processed(PRE_ACKNOWLEDGMENT_CS, input);
        return CompletableFuture.completedFuture(Message.of(input.getPayload() + "1"));
    }

    @Outgoing(PRE_ACKNOWLEDGMENT_CS)
    public Publisher<Message<String>> sourceToPreAckCS() {
        return Multi.createFrom().items("a", "b", "c", "d", "e")
                .map(payload -> Message.of(payload, () -> {
                    nap();
                    acknowledged(PRE_ACKNOWLEDGMENT_CS, payload);
                    return CompletableFuture.completedFuture(null);
                }));
    }

    @Incoming(PRE_ACKNOWLEDGMENT_UNI)
    @Acknowledgment(Acknowledgment.Strategy.PRE_PROCESSING)
    @Outgoing("sink")
    public Uni<Message<String>> processorWithPreAckUni(Message<String> input) {
        processed(PRE_ACKNOWLEDGMENT_UNI, input);
        return Uni.createFrom().completionStage(() -> CompletableFuture.completedFuture(Message.of(input.getPayload() + "1")));
    }

    @Outgoing(PRE_ACKNOWLEDGMENT_UNI)
    public Publisher<Message<String>> sourceToPreAckUni() {
        return Multi.createFrom().items("a", "b", "c", "d", "e")
                .map(payload -> Message.of(payload, () -> {
                    nap();
                    acknowledged(PRE_ACKNOWLEDGMENT_UNI, payload);
                    return CompletableFuture.completedFuture(null);
                }));
    }

    @Incoming(DEFAULT_ACKNOWLEDGMENT)
    @Outgoing("sink")
    public Message<String> processorWithDefaultAck(Message<String> input) {
        processed(DEFAULT_ACKNOWLEDGMENT, input);
        return input.withPayload(input.getPayload() + "1");
    }

    @Outgoing(DEFAULT_ACKNOWLEDGMENT)
    public Publisher<Message<String>> sourceToDefaultAck() {
        return Multi.createFrom().items("a", "b", "c", "d", "e")
                .map(payload -> Message.of(payload, () -> {
                    nap();
                    acknowledged(DEFAULT_ACKNOWLEDGMENT, payload);
                    return CompletableFuture.completedFuture(null);
                }));
    }

    @Incoming(DEFAULT_ACKNOWLEDGMENT_CS)
    @Outgoing("sink")
    public CompletionStage<Message<String>> processorWithDefaultAckCS(Message<String> input) {
        processed(DEFAULT_ACKNOWLEDGMENT_CS, input);
        return CompletableFuture.completedFuture(input.withPayload(input.getPayload() + "1"));
    }

    @Outgoing(DEFAULT_ACKNOWLEDGMENT_CS)
    public Publisher<Message<String>> sourceToDefaultAckCS() {
        return Multi.createFrom().items("a", "b", "c", "d", "e")
                .map(payload -> Message.of(payload, () -> {
                    nap();
                    acknowledged(DEFAULT_ACKNOWLEDGMENT_CS, payload);
                    return CompletableFuture.completedFuture(null);
                }));
    }

    @Incoming(DEFAULT_ACKNOWLEDGMENT_UNI)
    @Outgoing("sink")
    public Uni<Message<String>> processorWithDefaultAckUni(Message<String> input) {
        return Uni.createFrom().item(() -> {
            processed(DEFAULT_ACKNOWLEDGMENT_UNI, input);
            return input.withPayload(input.getPayload() + "1");
        });
    }

    @Outgoing(DEFAULT_ACKNOWLEDGMENT_UNI)
    public Publisher<Message<String>> sourceToDefaultAckUni() {
        return Multi.createFrom().items("a", "b", "c", "d", "e")
                .map(payload -> Message.of(payload, () -> {
                    nap();
                    acknowledged(DEFAULT_ACKNOWLEDGMENT_UNI, payload);
                    return CompletableFuture.completedFuture(null);
                }));
    }

}
