package io.smallrye.reactive.messaging.ack;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.reactivestreams.Publisher;

import io.reactivex.Flowable;
import io.smallrye.reactive.messaging.annotations.Merge;

@ApplicationScoped
public class BeanWithProcessorsManipulatingMessages extends SpiedBeanHelper {

    static final String MANUAL_ACKNOWLEDGMENT_CS = "manual-acknowledgment-cs";

    static final String NO_ACKNOWLEDGMENT = "no-acknowledgment";
    static final String NO_ACKNOWLEDGMENT_CS = "no-acknowledgment-cs";

    static final String PRE_ACKNOWLEDGMENT = "pre-acknowledgment";
    static final String PRE_ACKNOWLEDGMENT_CS = "pre-acknowledgment-cs";

    static final String DEFAULT_ACKNOWLEDGMENT = "default-acknowledgment";
    static final String DEFAULT_ACKNOWLEDGMENT_CS = "default-acknowledgment-cs";

    @Incoming("sink")
    @Merge
    @Acknowledgment(Acknowledgment.Strategy.NONE)
    public CompletionStage<Void> justTheSink(Message<String> ignored) {
        return CompletableFuture.completedFuture(null);
    }

    @Incoming(MANUAL_ACKNOWLEDGMENT_CS)
    @Acknowledgment(Acknowledgment.Strategy.MANUAL)
    @Outgoing("sink")
    public CompletionStage<Message<String>> processorWithAck(Message<String> input) {
        processed(MANUAL_ACKNOWLEDGMENT_CS, input);
        return input.ack().thenApply(x -> Message.<String>newBuilder().payload(input.getPayload() + "1").build());
    }

    @Outgoing(MANUAL_ACKNOWLEDGMENT_CS)
    public Publisher<Message<String>> sourceToManualAck() {
        return ReactiveStreams.of("a", "b", "c", "d", "e")
                .map(payload -> Message.<String>newBuilder().payload(payload).ack(() -> CompletableFuture.runAsync(() -> {
                    nap();
                    acknowledged(MANUAL_ACKNOWLEDGMENT_CS, payload);
                })).build()).buildRs();
    }

    @Incoming(NO_ACKNOWLEDGMENT)
    @Acknowledgment(Acknowledgment.Strategy.NONE)
    @Outgoing("sink")
    public Message<String> processorWithNoAck(Message<String> input) {
        processed(NO_ACKNOWLEDGMENT, input);
        return Message.<String>newBuilder().payload(input.getPayload() + "1").build();
    }

    @Outgoing(NO_ACKNOWLEDGMENT)
    public Publisher<Message<String>> sourceToNoAck() {
        return Flowable.fromArray("a", "b", "c", "d", "e")
                .map(payload -> Message.<String>newBuilder().payload(payload).ack(() -> {
                    nap();
                    acknowledged(NO_ACKNOWLEDGMENT, payload);
                    return CompletableFuture.completedFuture(null);
                }).build());
    }

    @Incoming(NO_ACKNOWLEDGMENT_CS)
    @Acknowledgment(Acknowledgment.Strategy.NONE)
    @Outgoing("sink")
    public CompletionStage<Message<String>> processorWithNoAckCS(Message<String> input) {
        return CompletableFuture.completedFuture(input)
                .thenApply(m -> {
                    processed(NO_ACKNOWLEDGMENT_CS, input);
                    return Message.<String>newBuilder().payload(input.getPayload() + "1").build();
                });
    }

    @Outgoing(NO_ACKNOWLEDGMENT_CS)
    public Publisher<Message<String>> sourceToNoAckCS() {
        return Flowable.fromArray("a", "b", "c", "d", "e")
                .map(payload -> Message.<String>newBuilder().payload(payload).ack(() -> {
                    nap();
                    acknowledged(NO_ACKNOWLEDGMENT_CS, payload);
                    return CompletableFuture.completedFuture(null);
                }).build());
    }

    @Incoming(PRE_ACKNOWLEDGMENT)
    @Acknowledgment(Acknowledgment.Strategy.PRE_PROCESSING)
    @Outgoing("sink")
    public Message<String> processorWithPreAck(Message<String> input) {
        processed(PRE_ACKNOWLEDGMENT, input);
        return Message.<String>newBuilder().payload(input.getPayload() + "1").build();
    }

    @Outgoing(PRE_ACKNOWLEDGMENT)
    public Publisher<Message<String>> sourceToPreAck() {
        return Flowable.fromArray("a", "b", "c", "d", "e")
                .map(payload -> Message.<String>newBuilder().payload(payload).ack(() -> {
                    nap();
                    acknowledged(PRE_ACKNOWLEDGMENT, payload);
                    return CompletableFuture.completedFuture(null);
                }).build());
    }

    @Incoming(PRE_ACKNOWLEDGMENT_CS)
    @Acknowledgment(Acknowledgment.Strategy.PRE_PROCESSING)
    @Outgoing("sink")
    public CompletionStage<Message<String>> processorWithPreAckCS(Message<String> input) {
        processed(PRE_ACKNOWLEDGMENT_CS, input);
        return CompletableFuture.completedFuture(Message.<String>newBuilder().payload(input.getPayload() + "1").build());
    }

    @Outgoing(PRE_ACKNOWLEDGMENT_CS)
    public Publisher<Message<String>> sourceToPreAckCS() {
        return Flowable.fromArray("a", "b", "c", "d", "e")
                .map(payload -> Message.<String>newBuilder().payload(payload).ack(() -> {
                    nap();
                    acknowledged(PRE_ACKNOWLEDGMENT_CS, payload);
                    return CompletableFuture.completedFuture(null);
                }).build());
    }

    @Incoming(DEFAULT_ACKNOWLEDGMENT)
    @Outgoing("sink")
    public Message<String> processorWithDefaultAck(Message<String> input) {
        processed(DEFAULT_ACKNOWLEDGMENT, input);
        return Message.<String>newBuilder().payload(input.getPayload() + "1").build();
    }

    @Outgoing(DEFAULT_ACKNOWLEDGMENT)
    public Publisher<Message<String>> sourceToDefaultAck() {
        return Flowable.fromArray("a", "b", "c", "d", "e")
                .map(payload -> Message.<String>newBuilder().payload(payload).ack(() -> {
                    nap();
                    acknowledged(DEFAULT_ACKNOWLEDGMENT, payload);
                    return CompletableFuture.completedFuture(null);
                }).build());
    }

    @Incoming(DEFAULT_ACKNOWLEDGMENT_CS)
    @Outgoing("sink")
    public CompletionStage<Message<String>> processorWithDefaultAckCS(Message<String> input) {
        processed(DEFAULT_ACKNOWLEDGMENT_CS, input);
        return CompletableFuture.completedFuture(Message.<String>newBuilder().payload(input.getPayload() + "1").build());
    }

    @Outgoing(DEFAULT_ACKNOWLEDGMENT_CS)
    public Publisher<Message<String>> sourceToDefaultAckCS() {
        return Flowable.fromArray("a", "b", "c", "d", "e")
                .map(payload -> Message.<String>newBuilder().payload(payload).ack(() -> {
                    nap();
                    acknowledged(DEFAULT_ACKNOWLEDGMENT_CS, payload);
                    return CompletableFuture.completedFuture(null);
                }).build());
    }

}
