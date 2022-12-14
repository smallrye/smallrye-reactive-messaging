package io.smallrye.reactive.messaging.ack;

import static org.awaitility.Awaitility.await;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;

public class AckChainTest extends WeldTestBaseWithoutTails {

    @Test
    public void testWithMessagesUsingWithMethods() {
        addBeanClass(MyMessageProducer.class);
        addBeanClass(MyMessageConsumer.class);
        addBeanClass(MyMessageProcessorA.class);
        addBeanClass(MyMessageProcessorB.class);

        initialize();

        MyMessageProducer producer = get(MyMessageProducer.class);
        await().until(() -> producer.acks() == 20);
    }

    @Test
    public void testWithPayloads() {
        addBeanClass(MyMessageProducer.class);
        addBeanClass(MyMessageConsumer.class);
        addBeanClass(MyPayloadProcessorA.class);
        addBeanClass(MyPayloadProcessorB.class);

        initialize();

        MyMessageProducer producer = get(MyMessageProducer.class);
        await().until(() -> producer.acks() == 20);
    }

    @ApplicationScoped
    public static class MyMessageProducer {
        final AtomicInteger acks = new AtomicInteger();

        public int acks() {
            return acks.get();
        }

        @Outgoing("A")
        public Multi<Message<Integer>> generate() {
            return Multi.createFrom().range(0, 20)
                    .map(i -> Message.of(i, () -> {
                        acks.incrementAndGet();
                        return CompletableFuture.completedFuture(null);
                    }));
        }
    }

    @ApplicationScoped
    public static class MyMessageConsumer {

        @Incoming("C")
        public CompletionStage<Void> consume(Message<String> m) {
            return m.ack();
        }
    }

    @ApplicationScoped
    public static class MyMessageProcessorA {

        @Incoming("A")
        @Outgoing("B")
        public CompletionStage<Message<Integer>> process(Message<Integer> m) {
            return CompletableFuture.supplyAsync(() -> m.withPayload(m.getPayload() + 1));
        }
    }

    @ApplicationScoped
    public static class MyMessageProcessorB {

        @Incoming("B")
        @Outgoing("C")
        public Message<String> process(Message<Integer> m) {
            return m.withPayload(Integer.toString(m.getPayload()));
        }
    }

    @ApplicationScoped
    public static class MyPayloadProcessorA {

        @Incoming("A")
        @Outgoing("B")
        public CompletionStage<Integer> process(int p) {
            return CompletableFuture.supplyAsync(() -> p + 1);
        }
    }

    @ApplicationScoped
    public static class MyPayloadProcessorB {

        @Incoming("B")
        @Outgoing("C")
        public String process(int p) {
            return Integer.toString(p);
        }
    }

}
