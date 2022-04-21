package io.smallrye.reactive.messaging.ack;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.reactivestreams.Publisher;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

@ApplicationScoped
public class BeanWithProcessorsManipulatingPayloads extends SpiedBeanHelper {

    static final String NO_ACKNOWLEDGMENT = "no-acknowledgment";
    static final String NO_ACKNOWLEDGMENT_CS = "no-acknowledgment-cs";
    static final String NO_ACKNOWLEDGMENT_UNI = "no-acknowledgment-uni";

    static final String PRE_ACKNOWLEDGMENT = "pre-acknowledgment";
    static final String PRE_ACKNOWLEDGMENT_CS = "pre-acknowledgment-cs";
    static final String PRE_ACKNOWLEDGMENT_UNI = "pre-acknowledgment-uni";

    static final String POST_ACKNOWLEDGMENT = "post-acknowledgment";
    static final String POST_ACKNOWLEDGMENT_CS = "post-acknowledgment-cs";
    static final String POST_ACKNOWLEDGMENT_UNI = "post-acknowledgment-uni";

    static final String DEFAULT_ACKNOWLEDGMENT = "default-acknowledgment";
    static final String DEFAULT_ACKNOWLEDGMENT_CS = "default-acknowledgment-cs";
    static final String DEFAULT_ACKNOWLEDGMENT_UNI = "default-acknowledgment-uni";

    @Incoming("sink-" + NO_ACKNOWLEDGMENT_CS)
    @Acknowledgment(Acknowledgment.Strategy.NONE)
    public CompletionStage<Void> sinkNoCS(Message<String> ignored) {
        return CompletableFuture.completedFuture(null);
    }

    @Incoming("sink-" + NO_ACKNOWLEDGMENT_UNI)
    @Acknowledgment(Acknowledgment.Strategy.NONE)
    public CompletionStage<Void> sinkNoUNI(Message<String> ignored) {
        return CompletableFuture.completedFuture(null);
    }

    @Incoming("sink-" + NO_ACKNOWLEDGMENT)
    @Acknowledgment(Acknowledgment.Strategy.NONE)
    public CompletionStage<Void> sinkNo(Message<String> ignored) {
        return CompletableFuture.completedFuture(null);
    }

    @Incoming("sink-" + PRE_ACKNOWLEDGMENT)
    @Acknowledgment(Acknowledgment.Strategy.NONE)
    public CompletionStage<Void> sinkPre(Message<String> ignored) {
        return CompletableFuture.completedFuture(null);
    }

    @Incoming("sink-" + PRE_ACKNOWLEDGMENT_CS)
    @Acknowledgment(Acknowledgment.Strategy.NONE)
    public CompletionStage<Void> sinkPreCS(Message<String> ignored) {
        return CompletableFuture.completedFuture(null);
    }

    @Incoming("sink-" + PRE_ACKNOWLEDGMENT_UNI)
    @Acknowledgment(Acknowledgment.Strategy.NONE)
    public CompletionStage<Void> sinkPreUni(Message<String> ignored) {
        return CompletableFuture.completedFuture(null);
    }

    @Incoming("sink-" + POST_ACKNOWLEDGMENT)
    @Acknowledgment(Acknowledgment.Strategy.POST_PROCESSING)
    public CompletionStage<Void> sinkPost(Message<String> ignored) {
        return CompletableFuture.completedFuture(null);
    }

    @Incoming("sink-" + POST_ACKNOWLEDGMENT_CS)
    @Acknowledgment(Acknowledgment.Strategy.POST_PROCESSING)
    public CompletionStage<Void> sinkPostCS(Message<String> ignored) {
        return CompletableFuture.completedFuture(null);
    }

    @Incoming("sink-" + POST_ACKNOWLEDGMENT_UNI)
    @Acknowledgment(Acknowledgment.Strategy.POST_PROCESSING)
    public CompletionStage<Void> sinkPostUni(Message<String> ignored) {
        return CompletableFuture.completedFuture(null);
    }

    @Incoming("sink-" + DEFAULT_ACKNOWLEDGMENT)
    @Acknowledgment(Acknowledgment.Strategy.POST_PROCESSING)
    public CompletionStage<Void> sinkDefault(Message<String> ignored) {
        return CompletableFuture.completedFuture(null);
    }

    @Incoming("sink-" + DEFAULT_ACKNOWLEDGMENT_CS)
    @Acknowledgment(Acknowledgment.Strategy.POST_PROCESSING)
    public CompletionStage<Void> sinkDefaultCS(Message<String> ignored) {
        return CompletableFuture.completedFuture(null);
    }

    @Incoming("sink-" + DEFAULT_ACKNOWLEDGMENT_UNI)
    @Acknowledgment(Acknowledgment.Strategy.POST_PROCESSING)
    public CompletionStage<Void> sinkDefaultUni(Message<String> ignored) {
        return CompletableFuture.completedFuture(null);
    }

    @Incoming(NO_ACKNOWLEDGMENT)
    @Acknowledgment(Acknowledgment.Strategy.NONE)
    @Outgoing("sink-" + NO_ACKNOWLEDGMENT)
    public String processorWithNoAck(String input) {
        processed(NO_ACKNOWLEDGMENT, input);
        return input + "1";
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
    @Outgoing("sink-" + NO_ACKNOWLEDGMENT_CS)
    public CompletionStage<String> processorWithNoAckCS(String input) {
        return CompletableFuture.completedFuture(input)
                .thenApply(m -> {
                    processed(NO_ACKNOWLEDGMENT_CS, input);
                    return m + "1";
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
    @Outgoing("sink-" + NO_ACKNOWLEDGMENT_UNI)
    public Uni<String> processorWithNoAckUni(String input) {
        return Uni.createFrom().item(() -> {
            processed(NO_ACKNOWLEDGMENT_UNI, input);
            return input + "1";
        });
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
    @Outgoing("sink-" + PRE_ACKNOWLEDGMENT)
    public String processorWithPreAck(String input) {
        processed(PRE_ACKNOWLEDGMENT, input);
        return input + "1";
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
    @Outgoing("sink-" + PRE_ACKNOWLEDGMENT_CS)
    public CompletionStage<String> processorWithPreAckCS(String input) {
        processed(PRE_ACKNOWLEDGMENT_CS, input);
        return CompletableFuture.completedFuture(input + "1");
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
    @Outgoing("sink-" + PRE_ACKNOWLEDGMENT_UNI)
    public Uni<String> processorWithPreAckUNI(String input) {
        processed(PRE_ACKNOWLEDGMENT_UNI, input);
        return Uni.createFrom().item(() -> input + "1");
    }

    @Outgoing(PRE_ACKNOWLEDGMENT_UNI)
    public Publisher<Message<String>> sourceToPreAckUNI() {
        return Multi.createFrom().items("a", "b", "c", "d", "e")
                .map(payload -> Message.of(payload, () -> {
                    nap();
                    acknowledged(PRE_ACKNOWLEDGMENT_UNI, payload);
                    return CompletableFuture.completedFuture(null);
                }));
    }

    @Incoming(POST_ACKNOWLEDGMENT)
    @Acknowledgment(Acknowledgment.Strategy.POST_PROCESSING)
    @Outgoing("sink-" + POST_ACKNOWLEDGMENT)
    public String processorWithPostAck(String input) {
        processed(POST_ACKNOWLEDGMENT, input);
        return input + "1";
    }

    @Outgoing(POST_ACKNOWLEDGMENT)
    public Publisher<Message<String>> sourceToPostAck() {
        return Multi.createFrom().items("a", "b", "c", "d", "e")
                .map(payload -> Message.of(payload, () -> {
                    nap();
                    acknowledged(POST_ACKNOWLEDGMENT, payload);
                    return CompletableFuture.completedFuture(null);
                }));
    }

    @Incoming(POST_ACKNOWLEDGMENT_CS)
    @Acknowledgment(Acknowledgment.Strategy.POST_PROCESSING)
    @Outgoing("sink-" + POST_ACKNOWLEDGMENT_CS)
    public CompletionStage<String> processorWithPostAckCS(String input) {
        processed(POST_ACKNOWLEDGMENT_CS, input);
        return CompletableFuture.completedFuture(input + "1");
    }

    @Outgoing(POST_ACKNOWLEDGMENT_CS)
    public Publisher<Message<String>> sourceToPostCSAck() {
        return Multi.createFrom().items("a", "b", "c", "d", "e")
                .map(payload -> Message.of(payload, () -> {
                    nap();
                    acknowledged(POST_ACKNOWLEDGMENT_CS, payload);
                    return CompletableFuture.completedFuture(null);
                }));
    }

    @Incoming(POST_ACKNOWLEDGMENT_UNI)
    @Acknowledgment(Acknowledgment.Strategy.POST_PROCESSING)
    @Outgoing("sink-" + POST_ACKNOWLEDGMENT_UNI)
    public Uni<String> processorWithPostAckUNI(String input) {
        processed(POST_ACKNOWLEDGMENT_UNI, input);
        return Uni.createFrom().item(() -> input + "1");
    }

    @Outgoing(POST_ACKNOWLEDGMENT_UNI)
    public Publisher<Message<String>> sourceToPostUNIAck() {
        return Multi.createFrom().items("a", "b", "c", "d", "e")
                .map(payload -> Message.of(payload, () -> {
                    nap();
                    acknowledged(POST_ACKNOWLEDGMENT_UNI, payload);
                    return CompletableFuture.completedFuture(null);
                }));
    }

    @Incoming(DEFAULT_ACKNOWLEDGMENT)
    @Outgoing("sink-" + DEFAULT_ACKNOWLEDGMENT)
    public String processorWithDefaultAck(String input) {
        processed(DEFAULT_ACKNOWLEDGMENT, input);
        return input + "1";
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
    @Outgoing("sink-" + DEFAULT_ACKNOWLEDGMENT_CS)
    public CompletionStage<String> processorWithDefaultAckCS(String input) {
        processed(DEFAULT_ACKNOWLEDGMENT_CS, input);
        return CompletableFuture.completedFuture(input + "1");
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
    @Outgoing("sink-" + DEFAULT_ACKNOWLEDGMENT_UNI)
    public Uni<String> processorWithDefaultAckUNI(String input) {
        processed(DEFAULT_ACKNOWLEDGMENT_UNI, input);
        return Uni.createFrom().item(input + "1");
    }

    @Outgoing(DEFAULT_ACKNOWLEDGMENT_UNI)
    public Publisher<Message<String>> sourceToDefaultAckUNI() {
        return Multi.createFrom().items("a", "b", "c", "d", "e")
                .map(payload -> Message.of(payload, () -> {
                    nap();
                    acknowledged(DEFAULT_ACKNOWLEDGMENT_UNI, payload);
                    return CompletableFuture.completedFuture(null);
                }));
    }

}
