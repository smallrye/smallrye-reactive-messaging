package io.smallrye.reactive.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.Multi;

public class OutgoingInterceptorTest extends WeldTestBaseWithoutTails {

    @BeforeEach
    void setupConfig() {
        installConfig("src/test/resources/config/interceptor.properties");
    }

    @Test
    public void testOutgoingInterceptorWithIdentifier() {
        addBeanClass(DefaultInterceptor.class);
        addBeanClass(InterceptorBean.class);
        addBeanClass(MyMessageProducer.class);

        initialize();

        InterceptorBean interceptor = container.getBeanManager().createInstance()
                .select(InterceptorBean.class, Identifier.Literal.of("A")).get();
        await().until(() -> interceptor.acks() == 10);
        await().until(() -> interceptor.nacks() == 10);
        assertThat(interceptor.interceptedMessages()).isEqualTo(20);
        assertThat(interceptor.interceptedResults()).containsExactlyElementsOf(
                IntStream.range(1, 11).boxed().collect(Collectors.toList()));
    }

    @Test
    public void testOutgoingInterceptorWithDefault() {
        addBeanClass(DefaultInterceptor.class);
        addBeanClass(MyMessageProducer.class);

        initialize();

        DefaultInterceptor interceptor = get(DefaultInterceptor.class);
        await().until(() -> interceptor.acks() == 10);
        await().until(() -> interceptor.nacks() == 10);
    }

    @ApplicationScoped
    public static class MyMessageProducer {

        @Outgoing("A")
        public Multi<Message<Integer>> generate() {
            return Multi.createFrom().range(0, 20)
                    .map(i -> Message.of(i, () -> {
                        CompletableFuture<Void> future = new CompletableFuture<>();
                        if (i >= 10) {
                            future.completeExceptionally(new IllegalStateException());
                        } else {
                            future.complete(null);
                        }
                        return future;
                    }));
        }
    }

    @Identifier("A")
    @ApplicationScoped
    static class InterceptorBean implements OutgoingInterceptor {

        final AtomicInteger acks = new AtomicInteger();
        final AtomicInteger nacks = new AtomicInteger();
        final AtomicInteger interceptedMessages = new AtomicInteger();
        final List<Integer> interceptedResults = new CopyOnWriteArrayList<>();

        @Override
        public Message<?> onMessage(Message<?> message) {
            interceptedMessages.incrementAndGet();
            Message<Integer> integerMessage = (Message<Integer>) message;
            return message.withPayload(integerMessage.getPayload() + 1);
        }

        @Override
        public void onMessageAck(Message<?> message) {
            acks.incrementAndGet();
            Object result = message.getMetadata(OutgoingMessageMetadata.class)
                    .map(OutgoingMessageMetadata::getResult)
                    .orElse(null);
            interceptedResults.add((Integer) result);
        }

        @Override
        public void onMessageNack(Message<?> message, Throwable failure) {
            nacks.incrementAndGet();
        }

        public int interceptedMessages() {
            return interceptedMessages.get();
        }

        public int acks() {
            return acks.get();
        }

        public int nacks() {
            return nacks.get();
        }

        public List<Integer> interceptedResults() {
            return interceptedResults;
        }
    }

    @ApplicationScoped
    static class DefaultInterceptor implements OutgoingInterceptor {

        final AtomicInteger acks = new AtomicInteger();
        final AtomicInteger nacks = new AtomicInteger();

        @Override
        public void onMessageAck(Message<?> message) {
            acks.incrementAndGet();
        }

        @Override
        public void onMessageNack(Message<?> message, Throwable failure) {
            nacks.incrementAndGet();
        }

        public int acks() {
            return acks.get();
        }

        public int nacks() {
            return nacks.get();
        }
    }

}
