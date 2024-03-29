package io.smallrye.reactive.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.smallrye.common.annotation.Identifier;

public class IncomingInterceptorTest extends WeldTestBaseWithoutTails {

    @BeforeEach
    void setupConfig() {
        installConfig("src/test/resources/config/interceptor.properties");
    }

    @Test
    public void testIncomingInterceptorWithIdentifier() {
        addBeanClass(DefaultInterceptor.class);
        addBeanClass(InterceptorBean.class);
        addBeanClass(MyMessageConsumer.class);

        initialize();

        InterceptorBean interceptor = container.getBeanManager().createInstance()
                .select(InterceptorBean.class, Identifier.Literal.of("B")).get();
        MyMessageConsumer consumerBean = get(MyMessageConsumer.class);
        await().until(() -> interceptor.acks() == 2);
        await().until(() -> interceptor.nacks() == 1);
        assertThat(interceptor.interceptedMessages()).isEqualTo(3);
        assertThat(consumerBean.received())
                .isNotEmpty()
                .allSatisfy(o -> assertThat(o).isInstanceOf(InterceptorBean.class));
        assertThat(interceptor.receivedMetadata())
                .isNotEmpty()
                .allSatisfy(o -> assertThat(o).isInstanceOf(MyMessageConsumer.class));
    }

    @Test
    public void testIncomingInterceptorWithDefault() {
        addBeanClass(DefaultInterceptor.class);
        addBeanClass(MyMessageConsumer.class);

        initialize();

        DefaultInterceptor interceptor = get(DefaultInterceptor.class);
        await().until(() -> interceptor.acks() == 2);
        await().until(() -> interceptor.nacks() == 1);
    }

    @ApplicationScoped
    public static class MyMessageConsumer {

        List<Object> receivedMetadata = new CopyOnWriteArrayList<>();

        @Incoming("B")
        public CompletionStage<Void> consume(Message<Integer> msg) {
            msg.getMetadata(InterceptorBean.class).ifPresent(receivedMetadata::add);
            if (msg.getPayload() == 3) {
                return msg.nack(new RuntimeException("boom!"), Metadata.of(this));
            }
            return msg.ack(Metadata.of(this));
        }

        public List<Object> received() {
            return receivedMetadata;
        }
    }

    @Identifier("B")
    @ApplicationScoped
    static class InterceptorBean implements IncomingInterceptor {

        final AtomicInteger acks = new AtomicInteger();
        final AtomicInteger nacks = new AtomicInteger();
        final AtomicInteger interceptedMessages = new AtomicInteger();

        final List<Object> receivedMetadata = new CopyOnWriteArrayList<>();

        @Override
        public Message<?> afterMessageReceive(Message<?> message) {
            interceptedMessages.incrementAndGet();
            return message.addMetadata(this)
                    .withNack(throwable -> {
                        System.out.println("nack1");
                        return message.nack(throwable);
                    }).thenApply(msg -> msg.withNackWithMetadata((throwable, metadata) -> msg.nack(throwable, metadata)
                            .thenAccept(unused -> {
                                System.out.println(throwable.getMessage());
                                for (Object metadatum : metadata) {
                                    System.out.println("nack2 " + metadatum.getClass());
                                    receivedMetadata.add(metadatum);
                                }
                            })))
                    .thenApply(msg -> msg.withAckWithMetadata(metadata -> {
                        for (Object metadatum : metadata) {
                            System.out.println("ack " + metadatum);
                        }
                        return message.ack(metadata);
                    }));
        }

        @Override
        public void onMessageAck(Message<?> message) {
            acks.incrementAndGet();
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

        public List<Object> receivedMetadata() {
            return receivedMetadata;
        }

    }

    @ApplicationScoped
    static class DefaultInterceptor implements IncomingInterceptor {

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
