package io.smallrye.reactive.messaging.blocking;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.PreDestroy;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.spi.DeploymentException;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.junit.Test;
import org.reactivestreams.Subscriber;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import io.smallrye.reactive.messaging.annotations.Blocking;

public class InvalidBlockingSubscriberShapeTest extends WeldTestBaseWithoutTails {
    @Test(expected = DeploymentException.class)
    public void testBeanProducingASubscriberOfMessages() {
        addBeanClass(BeanReturningASubscriberOfMessages.class);
        initialize();
    }

    @ApplicationScoped
    public static class BeanReturningASubscriberOfMessages {
        private List<String> list = new ArrayList<>();

        @Blocking
        @Incoming("count")
        public Subscriber<Message<String>> create() {
            return ReactiveStreams.<Message<String>> builder().forEach(m -> list.add(m.getPayload()))
                    .build();
        }
    }

    @Test(expected = DeploymentException.class)
    public void testBeanProducingASubscriberOfPayloads() {
        addBeanClass(BeanReturningASubscriberOfPayloads.class);
        initialize();
    }

    @ApplicationScoped
    public static class BeanReturningASubscriberOfPayloads {
        private List<String> list = new ArrayList<>();

        @Blocking
        @Incoming("count")
        public Subscriber<String> create() {
            return ReactiveStreams.<String> builder().forEach(m -> list.add(m)).build();
        }
    }

    @Test(expected = DeploymentException.class)
    public void testThatWeCanProduceSubscriberOfMessage() {
        addBeanClass(BeanReturningASubscriberOfMessagesButDiscarding.class);
        initialize();
    }

    @ApplicationScoped
    public static class BeanReturningASubscriberOfMessagesButDiscarding {
        @Blocking
        @Incoming("subscriber")
        public Subscriber<Message<String>> create() {
            return ReactiveStreams.<Message<String>> builder()
                    .ignore().build();
        }
    }

    @Test(expected = DeploymentException.class)
    public void testThatWeCanConsumeMessagesFromAMethodReturningACompletionStageOfSomething() {
        addBeanClass(BeanConsumingMessagesAndReturningACompletionStageOfSomething.class);
        initialize();
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

    @Test(expected = DeploymentException.class)
    public void testThatWeCanConsumePayloadsFromAMethodReturningACompletionStageOfSomething() {
        addBeanClass(BeanConsumingPayloadsAndReturningACompletionStageOfSomething.class);
        initialize();
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

    @Test(expected = DeploymentException.class)
    public void testThatWeCanConsumeMessagesFromAMethodReturningUniOfSomething() {
        addBeanClass(BeanConsumingMessagesAndReturningUniOfSomething.class);
        initialize();
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

    @Test(expected = DeploymentException.class)
    public void testThatWeCanConsumePayloadsFromAMethodReturningUniOfSomething() {
        addBeanClass(BeanConsumingPayloadsAndReturningUniOfSomething.class);
        initialize();
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
