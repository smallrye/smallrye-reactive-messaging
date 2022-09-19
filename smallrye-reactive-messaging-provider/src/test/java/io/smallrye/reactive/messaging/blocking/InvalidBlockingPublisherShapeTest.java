package io.smallrye.reactive.messaging.blocking;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.spi.DeploymentException;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;

import io.reactivex.Flowable;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import io.smallrye.reactive.messaging.annotations.Blocking;

public class InvalidBlockingPublisherShapeTest extends WeldTestBaseWithoutTails {
    @Test
    public void testPublisherOfMessages() {
        addBeanClass(BeanReturningAPublisherOfMessages.class);
        assertThatThrownBy(this::initialize).isInstanceOf(DeploymentException.class);
    }

    @ApplicationScoped
    public static class BeanReturningAPublisherOfMessages {
        @Blocking
        @Outgoing("sink")
        public Publisher<Message<String>> create() {
            return ReactiveStreams.of("a", "b", "c").map(Message::of).buildRs();
        }
    }

    @Test
    public void testPublisherOfMessagesWithMulti() {
        addBeanClass(BeanProducingMessagesAsMulti.class);
        assertThatThrownBy(this::initialize).isInstanceOf(DeploymentException.class);
    }

    @ApplicationScoped
    public static class BeanProducingMessagesAsMulti {
        @Blocking
        @Outgoing("sink")
        public Multi<Message<String>> publisher() {
            return Multi.createFrom().range(1, 11).flatMap(i -> Flowable.just(i, i)).map(i -> Integer.toString(i))
                    .map(Message::of);
        }
    }

    @Test
    public void testProducingPayloadsAsMulti() {
        addBeanClass(BeanProducingPayloadAsMulti.class);
        assertThatThrownBy(this::initialize).isInstanceOf(DeploymentException.class);
    }

    @ApplicationScoped
    public static class BeanProducingPayloadAsMulti {
        @Blocking
        @Outgoing("sink")
        public Multi<String> publisher() {
            return Multi.createFrom().range(1, 11).flatMap(i -> Flowable.just(i, i)).map(i -> Integer.toString(i));
        }
    }

    @Test
    public void testProducingPayloadsAsPublisher() {
        addBeanClass(BeanProducingPayloadAsPublisher.class);
        assertThatThrownBy(this::initialize).isInstanceOf(DeploymentException.class);
    }

    @ApplicationScoped
    public static class BeanProducingPayloadAsPublisher {
        @Blocking
        @Outgoing("sink")
        public Publisher<String> publisher() {
            return Flowable.range(1, 10).flatMap(i -> Flowable.just(i, i)).map(i -> Integer.toString(i));
        }
    }

    @Test
    public void testProducingMessagesAsPublisherBuilder() {
        addBeanClass(BeanProducingMessagesAsPublisherBuilder.class);
        assertThatThrownBy(this::initialize).isInstanceOf(DeploymentException.class);
    }

    @ApplicationScoped
    public static class BeanProducingMessagesAsPublisherBuilder {
        @Blocking
        @Outgoing("sink")
        public PublisherBuilder<Message<String>> publisher() {
            return ReactiveStreams.fromPublisher(Flowable.range(1, 10))
                    .flatMapRsPublisher(i -> Flowable.just(i, i))
                    .map(i -> Integer.toString(i))
                    .map(Message::of);
        }
    }

    @Test
    public void testProducingPayloadAsPublisherBuilder() {
        addBeanClass(BeanProducingPayloadAsPublisherBuilder.class);
        assertThatThrownBy(this::initialize).isInstanceOf(DeploymentException.class);
    }

    @ApplicationScoped
    public static class BeanProducingPayloadAsPublisherBuilder {
        @Blocking
        @Outgoing("sink")
        public PublisherBuilder<String> publisher() {
            return ReactiveStreams.fromPublisher(Flowable.range(1, 10))
                    .flatMapRsPublisher(i -> Flowable.just(i, i))
                    .map(i -> Integer.toString(i));
        }
    }

    @Test
    public void testProduceCompletionStageOfMessage() {
        addBeanClass(BeanReturningCompletionStageOfMessage.class);
        assertThatThrownBy(this::initialize).isInstanceOf(DeploymentException.class);
    }

    @ApplicationScoped
    public static class BeanReturningCompletionStageOfMessage {
        private AtomicInteger count = new AtomicInteger();
        private ExecutorService executor = Executors.newSingleThreadExecutor();

        @Blocking
        @Outgoing("infinite-producer")
        public CompletionStage<Message<Integer>> create() {
            return CompletableFuture.supplyAsync(() -> Message.of(count.incrementAndGet()), executor);
        }

        @PreDestroy
        public void cleanup() {
            executor.shutdown();
        }
    }

    @Test
    public void testProduceCompletionStageOfPayload() {
        addBeanClass(BeanReturningCompletionStageOfPayload.class);
        assertThatThrownBy(this::initialize).isInstanceOf(DeploymentException.class);
    }

    @ApplicationScoped
    public static class BeanReturningCompletionStageOfPayload {
        private AtomicInteger count = new AtomicInteger();
        private ExecutorService executor = Executors.newSingleThreadExecutor();

        @Blocking
        @Outgoing("infinite-producer")
        public CompletionStage<Integer> create() {
            return CompletableFuture.supplyAsync(() -> count.incrementAndGet(), executor);
        }

        @PreDestroy
        public void cleanup() {
            executor.shutdown();
        }
    }

    @Test
    public void testProduceUniOfMessage() {
        addBeanClass(BeanReturningUniOfMessage.class);
        assertThatThrownBy(this::initialize).isInstanceOf(DeploymentException.class);
    }

    @ApplicationScoped
    public static class BeanReturningUniOfMessage {
        private AtomicInteger count = new AtomicInteger();

        @Blocking
        @Outgoing("infinite-producer")
        public Uni<Message<Integer>> create() {
            return Uni.createFrom().item(() -> Message.of(count.incrementAndGet()));
        }
    }

    @Test
    public void testProduceUniOfPayload() {
        addBeanClass(BeanReturningUniOfPayload.class);
        assertThatThrownBy(this::initialize).isInstanceOf(DeploymentException.class);
    }

    @ApplicationScoped
    public static class BeanReturningUniOfPayload {
        private AtomicInteger count = new AtomicInteger();

        @Blocking
        @Outgoing("infinite-producer")
        public Uni<Integer> create() {
            return Uni.createFrom().item(() -> count.incrementAndGet());
        }
    }
}
