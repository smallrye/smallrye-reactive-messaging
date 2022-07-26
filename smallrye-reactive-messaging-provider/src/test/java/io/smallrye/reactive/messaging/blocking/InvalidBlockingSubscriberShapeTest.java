package io.smallrye.reactive.messaging.blocking;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import javax.annotation.PreDestroy;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.spi.DeploymentException;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import io.smallrye.reactive.messaging.annotations.Blocking;
import mutiny.zero.flow.adapters.AdaptersToFlow;

public class InvalidBlockingSubscriberShapeTest extends WeldTestBaseWithoutTails {
    @Test
    public void testBeanProducingASubscriberOfMessages() {
        addBeanClass(BeanReturningASubscriberOfMessages.class);
        assertThatThrownBy(this::initialize).isInstanceOf(DeploymentException.class);
    }

    @ApplicationScoped
    public static class BeanReturningASubscriberOfMessages {
        private List<String> list = new ArrayList<>();

        @Blocking
        @Incoming("count")
        public Flow.Subscriber<Message<String>> create() {
            return AdaptersToFlow.subscriber(ReactiveStreams.<Message<String>> builder().forEach(m -> list.add(m.getPayload()))
                    .build());
        }
    }

    @Test
    public void testBeanProducingASubscriberOfPayloads() {
        addBeanClass(BeanReturningASubscriberOfPayloads.class);
        assertThatThrownBy(this::initialize).isInstanceOf(DeploymentException.class);
    }

    @ApplicationScoped
    public static class BeanReturningASubscriberOfPayloads {
        private List<String> list = new ArrayList<>();

        @Blocking
        @Incoming("count")
        public Flow.Subscriber<String> create() {
            return AdaptersToFlow.subscriber(ReactiveStreams.<String> builder().forEach(m -> list.add(m)).build());
        }
    }

    @Test
    public void testThatWeCanProduceSubscriberOfMessage() {
        addBeanClass(BeanReturningASubscriberOfMessagesButDiscarding.class);
        assertThatThrownBy(this::initialize).isInstanceOf(DeploymentException.class);
    }

    @ApplicationScoped
    public static class BeanReturningASubscriberOfMessagesButDiscarding {
        @Blocking
        @Incoming("subscriber")
        public Flow.Subscriber<Message<String>> create() {
            return AdaptersToFlow.subscriber(ReactiveStreams.<Message<String>> builder()
                    .ignore().build());
        }
    }

    @Test
    public void testThatWeCanConsumeMessagesFromAMethodReturningACompletionStageOfSomething() {
        addBeanClass(BeanConsumingMessagesAndReturningACompletionStageOfSomething.class);
        assertThatThrownBy(this::initialize).isInstanceOf(DeploymentException.class);
    }

    @ApplicationScoped
    public static class BeanConsumingMessagesAndReturningACompletionStageOfSomething {
        private List<String> list = new CopyOnWriteArrayList<>();
        private ExecutorService executor = Executors.newSingleThreadExecutor();

        @Blocking
        @Incoming("count")
        public CompletionStage<String> consume(Message<String> msg) {
            return CompletableFuture.supplyAsync(() -> {
                list.add(msg.getPayload());
                return "hello";
            }, executor);
        }

        @PreDestroy
        public void cleanup() {
            executor.shutdown();
        }
    }

    @Test
    public void testThatWeCanConsumePayloadsFromAMethodReturningACompletionStageOfSomething() {
        addBeanClass(BeanConsumingPayloadsAndReturningACompletionStageOfSomething.class);
        assertThatThrownBy(this::initialize).isInstanceOf(DeploymentException.class);
    }

    @ApplicationScoped
    public static class BeanConsumingPayloadsAndReturningACompletionStageOfSomething {
        private List<String> list = new CopyOnWriteArrayList<>();
        private ExecutorService executor = Executors.newSingleThreadExecutor();

        @Blocking
        @Incoming("count")
        public CompletionStage<String> consume(String payload) {
            return CompletableFuture.supplyAsync(() -> {
                list.add(payload);
                return "hello";
            }, executor);
        }

        @PreDestroy
        public void cleanup() {
            executor.shutdown();
        }
    }

    @Test
    public void testThatWeCanConsumeMessagesFromAMethodReturningUniOfSomething() {
        addBeanClass(BeanConsumingMessagesAndReturningUniOfSomething.class);
        assertThatThrownBy(this::initialize).isInstanceOf(DeploymentException.class);
    }

    @ApplicationScoped
    public static class BeanConsumingMessagesAndReturningUniOfSomething {
        private List<String> list = new CopyOnWriteArrayList<>();

        @Blocking
        @Incoming("count")
        public Uni<String> consume(Message<String> msg) {
            return Uni.createFrom().item(() -> {
                list.add(msg.getPayload());
                return "hello";
            });
        }
    }

    @Test
    public void testThatWeCanConsumePayloadsFromAMethodReturningUniOfSomething() {
        addBeanClass(BeanConsumingPayloadsAndReturningUniOfSomething.class);
        assertThatThrownBy(this::initialize).isInstanceOf(DeploymentException.class);
    }

    @ApplicationScoped
    public static class BeanConsumingPayloadsAndReturningUniOfSomething {
        private List<String> list = new CopyOnWriteArrayList<>();

        @Blocking
        @Incoming("count")
        public Uni<String> consume(String payload) {
            return Uni.createFrom().item(() -> {
                list.add(payload);
                return "hello";
            });
        }
    }
}
