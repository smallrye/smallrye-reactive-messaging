package io.smallrye.reactive.messaging.ack;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.reactivestreams.Publisher;

import io.smallrye.mutiny.Multi;

@ApplicationScoped
public class BeanWithProcessorsProducingPayloadStreams extends SpiedBeanHelper {

    static final String NO_ACKNOWLEDGMENT = "no-acknowledgment";
    static final String NO_ACKNOWLEDGMENT_BUILDER = "no-acknowledgment-builder";

    static final String PRE_ACKNOWLEDGMENT = "pre-acknowledgment";
    static final String PRE_ACKNOWLEDGMENT_BUILDER = "pre-acknowledgment-builder";

    static final String DEFAULT_ACKNOWLEDGMENT = "def-acknowledgment";
    static final String DEFAULT_ACKNOWLEDGMENT_BUILDER = "def-acknowledgment-builder";

    @Incoming("sink-" + DEFAULT_ACKNOWLEDGMENT)
    @Acknowledgment(Acknowledgment.Strategy.NONE)
    public CompletionStage<Void> sinkDefault(Message<String> ignored) {
        return CompletableFuture.completedFuture(null);
    }

    @Incoming("sink-" + DEFAULT_ACKNOWLEDGMENT_BUILDER)
    @Acknowledgment(Acknowledgment.Strategy.NONE)
    public CompletionStage<Void> sinkDefaultBuilder(Message<String> ignored) {
        return CompletableFuture.completedFuture(null);
    }

    @Incoming("sink-" + NO_ACKNOWLEDGMENT)
    @Acknowledgment(Acknowledgment.Strategy.NONE)
    public CompletionStage<Void> sinkNo(Message<String> ignored) {
        return CompletableFuture.completedFuture(null);
    }

    @Incoming("sink-" + NO_ACKNOWLEDGMENT_BUILDER)
    @Acknowledgment(Acknowledgment.Strategy.NONE)
    public CompletionStage<Void> sinkNoBuilder(Message<String> ignored) {
        return CompletableFuture.completedFuture(null);
    }

    @Incoming("sink-" + PRE_ACKNOWLEDGMENT)
    @Acknowledgment(Acknowledgment.Strategy.NONE)
    public CompletionStage<Void> sinkPre(Message<String> ignored) {
        return CompletableFuture.completedFuture(null);
    }

    @Incoming("sink-" + PRE_ACKNOWLEDGMENT_BUILDER)
    @Acknowledgment(Acknowledgment.Strategy.NONE)
    public CompletionStage<Void> sinkPreBuilder(Message<String> ignored) {
        return CompletableFuture.completedFuture(null);
    }

    @Incoming(NO_ACKNOWLEDGMENT)
    @Acknowledgment(Acknowledgment.Strategy.NONE)
    @Outgoing("sink-" + NO_ACKNOWLEDGMENT)
    public Publisher<String> processorWithNoAck(String input) {
        return ReactiveStreams.of(input)
                .flatMap(m -> ReactiveStreams.of(m, m))
                .peek(m -> processed(NO_ACKNOWLEDGMENT, m))
                .buildRs();
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

    @Incoming(NO_ACKNOWLEDGMENT_BUILDER)
    @Acknowledgment(Acknowledgment.Strategy.NONE)
    @Outgoing("sink-" + NO_ACKNOWLEDGMENT_BUILDER)
    public PublisherBuilder<String> processorWithNoAckWithBuilder(String message) {
        return ReactiveStreams.of(message)
                .flatMap(m -> ReactiveStreams.of(m, m))
                .peek(m -> processed(NO_ACKNOWLEDGMENT_BUILDER, message));
    }

    @Outgoing(NO_ACKNOWLEDGMENT_BUILDER)
    public Publisher<Message<String>> sourceToNoAckWithBuilder() {
        return Multi.createFrom().items("a", "b", "c", "d", "e")
                .map(payload -> Message.of(payload, () -> {
                    nap();
                    acknowledged(NO_ACKNOWLEDGMENT_BUILDER, payload);
                    return CompletableFuture.completedFuture(null);
                }));
    }

    @Incoming(PRE_ACKNOWLEDGMENT)
    @Acknowledgment(Acknowledgment.Strategy.PRE_PROCESSING)
    @Outgoing("sink-" + PRE_ACKNOWLEDGMENT)
    public Publisher<String> processorWithPreAck(String input) {
        return ReactiveStreams.of(input)
                .flatMap(m -> ReactiveStreams.of(m, m))
                .peek(m -> processed(PRE_ACKNOWLEDGMENT, m))
                .buildRs();
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

    @Incoming(PRE_ACKNOWLEDGMENT_BUILDER)
    @Acknowledgment(Acknowledgment.Strategy.PRE_PROCESSING)
    @Outgoing("sink-" + PRE_ACKNOWLEDGMENT_BUILDER)
    public PublisherBuilder<String> processorWithPreAckBuilder(String input) {
        return ReactiveStreams.of(input)
                .flatMap(m -> ReactiveStreams.of(m, m))
                .peek(m -> processed(PRE_ACKNOWLEDGMENT_BUILDER, m));
    }

    @Outgoing(PRE_ACKNOWLEDGMENT_BUILDER)
    public Publisher<Message<String>> sourceToPreAckBuilder() {
        return Multi.createFrom().items("a", "b", "c", "d", "e")
                .map(payload -> Message.of(payload, () -> {
                    nap();
                    acknowledged(PRE_ACKNOWLEDGMENT_BUILDER, payload);
                    return CompletableFuture.completedFuture(null);
                }));
    }

    @Incoming(DEFAULT_ACKNOWLEDGMENT)
    @Outgoing("sink-" + DEFAULT_ACKNOWLEDGMENT)
    public Publisher<String> processorWithDefaultAck(String input) {
        return ReactiveStreams.of(input)
                .flatMap(m -> ReactiveStreams.of(m, m))
                .peek(m -> processed(DEFAULT_ACKNOWLEDGMENT, m))
                .buildRs();
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

    @Incoming(DEFAULT_ACKNOWLEDGMENT_BUILDER)
    @Outgoing("sink-" + DEFAULT_ACKNOWLEDGMENT_BUILDER)
    public PublisherBuilder<String> processorWithDefaultAckBuilder(String input) {
        return ReactiveStreams.of(input)
                .flatMap(m -> ReactiveStreams.of(m, m))
                .peek(m -> processed(DEFAULT_ACKNOWLEDGMENT_BUILDER, m));
    }

    @Outgoing(DEFAULT_ACKNOWLEDGMENT_BUILDER)
    public Publisher<Message<String>> sourceToDefaultAckBuilder() {
        return Multi.createFrom().items("a", "b", "c", "d", "e")
                .map(payload -> Message.of(payload, () -> {
                    nap();
                    acknowledged(DEFAULT_ACKNOWLEDGMENT_BUILDER, payload);
                    return CompletableFuture.completedFuture(null);
                }));
    }
}
