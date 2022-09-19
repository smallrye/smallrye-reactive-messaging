package io.smallrye.reactive.messaging.ack;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.operators.ProcessorBuilder;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;

import io.smallrye.mutiny.Multi;

@ApplicationScoped
public class BeanWithMessageProcessors extends SpiedBeanHelper {

    static final String DEFAULT_ACKNOWLEDGMENT = "default-acknowledgment";
    static final String DEFAULT_ACKNOWLEDGMENT_BUILDER = "default-acknowledgment-builder";

    static final String PRE_ACKNOWLEDGMENT = "pre-acknowledgment";
    static final String PRE_ACKNOWLEDGMENT_BUILDER = "pre-acknowledgment-builder";

    static final String MANUAL_ACKNOWLEDGMENT = "manual-acknowledgment";
    static final String MANUAL_ACKNOWLEDGMENT_BUILDER = "manual-acknowledgment-builder";

    static final String NO_ACKNOWLEDGMENT = "no-acknowledgment";
    static final String NO_ACKNOWLEDGMENT_BUILDER = "no-acknowledgment-builder";

    private Map<String, List<String>> sink = new ConcurrentHashMap<>();
    private Map<String, List<String>> acknowledged = new ConcurrentHashMap<>();

    @Incoming("sink-" + DEFAULT_ACKNOWLEDGMENT)
    @Acknowledgment(Acknowledgment.Strategy.NONE)
    public CompletionStage<Void> sinkDef(Message<String> ignored) {
        return CompletableFuture.completedFuture(null);
    }

    @Incoming("sink-" + DEFAULT_ACKNOWLEDGMENT_BUILDER)
    @Acknowledgment(Acknowledgment.Strategy.NONE)
    public CompletionStage<Void> sinkDefBuilder(Message<String> ignored) {
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

    @Incoming("sink-" + NO_ACKNOWLEDGMENT)
    @Acknowledgment(Acknowledgment.Strategy.NONE)
    public CompletionStage<Void> sinkNo(Message<String> ignored) {
        return CompletableFuture.completedFuture(null);
    }

    @Incoming("sink-" + NO_ACKNOWLEDGMENT_BUILDER)
    @Acknowledgment(Acknowledgment.Strategy.NONE)
    public CompletionStage<Void> sinkNoForBuilder(Message<String> ignored) {
        return CompletableFuture.completedFuture(null);
    }

    @Incoming("sink-" + MANUAL_ACKNOWLEDGMENT)
    @Acknowledgment(Acknowledgment.Strategy.NONE)
    public CompletionStage<Void> sinkManual(Message<String> ignored) {
        return CompletableFuture.completedFuture(null);
    }

    @Incoming("sink-" + MANUAL_ACKNOWLEDGMENT_BUILDER)
    @Acknowledgment(Acknowledgment.Strategy.NONE)
    public CompletionStage<Void> sinkManualForBuilder(Message<String> ignored) {
        return CompletableFuture.completedFuture(null);
    }

    @Incoming(MANUAL_ACKNOWLEDGMENT)
    @Acknowledgment(Acknowledgment.Strategy.MANUAL)
    @Outgoing("sink-" + MANUAL_ACKNOWLEDGMENT)
    public Processor<Message<String>, Message<String>> processorWithAck() {
        return ReactiveStreams.<Message<String>> builder()
                .flatMapCompletionStage(m -> m.ack().thenApply(x -> m))
                .flatMap(m -> ReactiveStreams.of(Message.of(m.getPayload()), Message.of(m.getPayload())))
                .peek(m -> processed(MANUAL_ACKNOWLEDGMENT, m))
                .buildRs();
    }

    @Outgoing(MANUAL_ACKNOWLEDGMENT)
    public Publisher<Message<String>> sourceToManualAck() {
        return ReactiveStreams.of("a", "b", "c", "d", "e")
                .map(payload -> Message.of(payload, () -> CompletableFuture.runAsync(() -> {
                    nap();
                    acknowledged(MANUAL_ACKNOWLEDGMENT, payload);
                }))).buildRs();
    }

    @Incoming(NO_ACKNOWLEDGMENT)
    @Acknowledgment(Acknowledgment.Strategy.NONE)
    @Outgoing("sink-" + NO_ACKNOWLEDGMENT)
    public Processor<Message<String>, Message<String>> processorWithNoAck() {
        return ReactiveStreams.<Message<String>> builder()
                .flatMap(m -> ReactiveStreams.of(Message.of(m.getPayload()), Message.of(m.getPayload())))
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

    @Incoming(DEFAULT_ACKNOWLEDGMENT)
    @Outgoing("sink-" + DEFAULT_ACKNOWLEDGMENT)
    public Processor<Message<String>, Message<String>> processorWithAutoAck() {
        return ReactiveStreams.<Message<String>> builder()
                .flatMap(m -> ReactiveStreams.of(Message.of(m.getPayload()), Message.of(m.getPayload())).onComplete(
                        m::ack))
                .peek(m -> processed(DEFAULT_ACKNOWLEDGMENT, m))
                .buildRs();
    }

    @Outgoing(DEFAULT_ACKNOWLEDGMENT)
    public Publisher<Message<String>> sourceToAutoAck() {
        return Multi.createFrom().items("a", "b", "c", "d", "e")
                .map(payload -> Message.of(payload, () -> {
                    nap();
                    acknowledged(DEFAULT_ACKNOWLEDGMENT, payload);
                    return CompletableFuture.completedFuture(null);
                }));
    }

    @Incoming(PRE_ACKNOWLEDGMENT)
    @Acknowledgment(Acknowledgment.Strategy.PRE_PROCESSING)
    @Outgoing("sink-" + PRE_ACKNOWLEDGMENT)
    public Processor<Message<String>, Message<String>> processorWithPreAck() {
        return ReactiveStreams.<Message<String>> builder()
                .flatMap(m -> ReactiveStreams.of(Message.of(m.getPayload()), Message.of(m.getPayload())))
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

    @Incoming(MANUAL_ACKNOWLEDGMENT_BUILDER)
    @Acknowledgment(Acknowledgment.Strategy.MANUAL)
    @Outgoing("sink-" + MANUAL_ACKNOWLEDGMENT_BUILDER)
    public ProcessorBuilder<Message<String>, Message<String>> processorWithAckWithBuilder() {
        return ReactiveStreams.<Message<String>> builder()
                .flatMapCompletionStage(m -> m.ack().thenApply(x -> m))
                .flatMap(m -> ReactiveStreams.of(Message.of(m.getPayload()), Message.of(m.getPayload())))
                .peek(m -> processed(MANUAL_ACKNOWLEDGMENT_BUILDER, m));
    }

    @Outgoing(MANUAL_ACKNOWLEDGMENT_BUILDER)
    public PublisherBuilder<Message<String>> sourceToManualAckWithBuilder() {
        return ReactiveStreams.of("a", "b", "c", "d", "e")
                .map(payload -> Message.of(payload, () -> CompletableFuture.runAsync(() -> {
                    nap();
                    acknowledged(MANUAL_ACKNOWLEDGMENT_BUILDER, payload);
                })));
    }

    @Incoming(NO_ACKNOWLEDGMENT_BUILDER)
    @Acknowledgment(Acknowledgment.Strategy.NONE)
    @Outgoing("sink-" + NO_ACKNOWLEDGMENT_BUILDER)
    public ProcessorBuilder<Message<String>, Message<String>> processorWithNoAckWithBuilder() {
        return ReactiveStreams.<Message<String>> builder()
                .flatMap(m -> ReactiveStreams.of(Message.of(m.getPayload()), Message.of(m.getPayload())))
                .peek(m -> processed(NO_ACKNOWLEDGMENT_BUILDER, m));
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

    @Incoming(DEFAULT_ACKNOWLEDGMENT_BUILDER)
    @Outgoing("sink-" + DEFAULT_ACKNOWLEDGMENT_BUILDER)
    public ProcessorBuilder<Message<String>, Message<String>> processorWithAutoAckBuilder() {
        return ReactiveStreams.<Message<String>> builder()
                .flatMap(m -> ReactiveStreams.of(Message.of(m.getPayload()), Message.of(m.getPayload())).onComplete(
                        m::ack))
                .peek(m -> processed(DEFAULT_ACKNOWLEDGMENT_BUILDER, m));
    }

    @Outgoing(DEFAULT_ACKNOWLEDGMENT_BUILDER)
    public Publisher<Message<String>> sourceToAutoAckWithBuilder() {
        return Multi.createFrom().items("a", "b", "c", "d", "e")
                .map(payload -> Message.of(payload, () -> {
                    nap();
                    acknowledged(DEFAULT_ACKNOWLEDGMENT_BUILDER, payload);
                    return CompletableFuture.completedFuture(null);
                }));
    }

    @Incoming(PRE_ACKNOWLEDGMENT_BUILDER)
    @Acknowledgment(Acknowledgment.Strategy.PRE_PROCESSING)
    @Outgoing("sink-" + PRE_ACKNOWLEDGMENT_BUILDER)
    public ProcessorBuilder<Message<String>, Message<String>> processorWithPreAckBuilder() {
        return ReactiveStreams.<Message<String>> builder()
                .flatMap(m -> ReactiveStreams.of(Message.of(m.getPayload()), Message.of(m.getPayload())))
                .peek(m -> processed(PRE_ACKNOWLEDGMENT_BUILDER, m));
    }

    @Outgoing(PRE_ACKNOWLEDGMENT_BUILDER)
    public Publisher<Message<String>> sourceToPreAckWithBuilder() {
        return Multi.createFrom().items("a", "b", "c", "d", "e")
                .map(payload -> Message.of(payload, () -> {
                    nap();
                    acknowledged(PRE_ACKNOWLEDGMENT_BUILDER, payload);
                    return CompletableFuture.completedFuture(null);
                }));
    }

}
