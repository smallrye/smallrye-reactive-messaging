package io.smallrye.reactive.messaging.acknowledgement;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.*;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.reactivestreams.FlowAdapters;
import org.reactivestreams.Publisher;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;

/**
 * Verify post-acknowledgement (and negative-acknowledgment) for method consuming a payload and returning a Multi, Publisher
 * or PublisherBuilder of payload.
 * Under the hood it verifies the behavior of
 * {@link io.smallrye.reactive.messaging.providers.helpers.AcknowledgementCoordinator}.
 */
public class PayloadToMultiAcknowledgmentTest extends WeldTestBaseWithoutTails {

    @ParameterizedTest
    @ValueSource(classes = { ProcessorUsingMulti.class, ProcessorUsingRSPublisher.class, ProcessorUsingFlowPublisher.class,
            ProcessorUsingPublisherBuilder.class })
    void testCoordinatedAcknowledgement(Class<?> clz) {
        addBeanClass(InAndOut.class, clz);

        initialize();

        InAndOut bean = get(InAndOut.class);

        AtomicBoolean ackM1 = new AtomicBoolean();
        AtomicBoolean ackM2 = new AtomicBoolean();
        AtomicInteger nack = new AtomicInteger();

        bean.emitter().send(Message.of("a")
                .withAck(() -> {
                    ackM1.set(true);
                    return CompletableFuture.completedFuture(null);
                })
                .withNack(t -> {
                    nack.incrementAndGet();
                    return CompletableFuture.completedFuture(null);
                }));

        bean.emitter().send(Message.of("b")
                .withAck(() -> {
                    ackM2.set(true);
                    return CompletableFuture.completedFuture(null);
                })
                .withNack(t -> {
                    nack.incrementAndGet();
                    return CompletableFuture.completedFuture(null);
                }));

        assertThat(ackM1).isFalse();
        assertThat(ackM2).isFalse();
        assertThat(nack).hasValue(0);

        assertThat(bean.list()).hasSize(4);

        bean.list().get(0).ack();
        bean.list().get(3).ack();

        assertThat(ackM1).isFalse();
        assertThat(ackM2).isFalse();

        bean.list().get(1).ack();
        assertThat(ackM1).isTrue();
        assertThat(ackM2).isFalse();

        bean.list().get(2).nack(new IOException("boom"));
        assertThat(ackM1).isTrue();
        assertThat(ackM2).isFalse();
        assertThat(ackM1).isTrue();
        assertThat(nack).hasValue(1);

        // Ignored signals
        bean.list().get(2).ack();
        bean.list().get(1).ack();
        bean.list().get(3).nack(new IOException("boom"));
        assertThat(ackM1).isTrue();
        assertThat(ackM2).isFalse();
        assertThat(ackM1).isTrue();
        assertThat(nack).hasValue(1);
    }

    @ApplicationScoped
    static class InAndOut {
        @Inject
        @Channel("input")
        Emitter<String> emitter;

        public Emitter<String> emitter() {
            return emitter;
        }

        private List<Message<String>> list = new CopyOnWriteArrayList<>();

        @Incoming("output")
        CompletionStage<Void> consume(Message<String> s) {
            list.add(s);
            return CompletableFuture.completedFuture(null);
        }

        List<Message<String>> list() {
            return list;
        }
    }

    @ApplicationScoped
    static class ProcessorUsingMulti {

        @Incoming("input")
        @Outgoing("output")
        @Acknowledgment(Acknowledgment.Strategy.POST_PROCESSING)
        Multi<String> process(String i) {
            return Multi.createFrom().items(i, i);
        }
    }

    @ApplicationScoped
    static class ProcessorUsingRSPublisher {

        @Incoming("input")
        @Outgoing("output")
        @Acknowledgment(Acknowledgment.Strategy.POST_PROCESSING)
        Publisher<String> process(String i) {
            return FlowAdapters.toPublisher(Multi.createFrom().items(i, i));
        }
    }

    @ApplicationScoped
    static class ProcessorUsingFlowPublisher {

        @Incoming("input")
        @Outgoing("output")
        @Acknowledgment(Acknowledgment.Strategy.POST_PROCESSING)
        Flow.Publisher<String> process(String i) {
            return Multi.createFrom().items(i, i);
        }
    }

    @ApplicationScoped
    static class ProcessorUsingPublisherBuilder {

        @Incoming("input")
        @Outgoing("output")
        @Acknowledgment(Acknowledgment.Strategy.POST_PROCESSING)
        PublisherBuilder<String> process(String i) {
            return ReactiveStreams.of(i, i);
        }
    }

}
