package io.smallrye.reactive.messaging.interceptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MediatorConfiguration;
import io.smallrye.reactive.messaging.ProcessingDecorator;
import io.smallrye.reactive.messaging.WeldTestBase;

public class ProcessingInterceptorTest extends WeldTestBase {

    @Test
    public void testInterceptorAppliesTimeoutAndNacks() {
        addBeanClass(TimeoutInterceptor.class, SlowSubscriberBean.class, NackTrackingSource.class);
        initialize();

        NackTrackingSource source = container.select(NackTrackingSource.class).get();

        // The interceptor applies a 200ms timeout, processing takes 2s,
        // so messages should be nacked (POST_PROCESSING ack).
        // All 5 messages should eventually be nacked since they all time out.
        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(source.getNacks()).hasSize(5));

        // Verify nack reason is a TimeoutException
        assertThat(source.getNackReasons().get(0)).isInstanceOf(io.smallrye.mutiny.TimeoutException.class);
    }

    @Test
    public void testInterceptorPassesThroughFastProcessing() {
        addBeanClass(TimeoutInterceptor.class, FastSubscriberBean.class, NackTrackingSource.class);
        initialize();

        FastSubscriberBean bean = container.select(FastSubscriberBean.class).get();
        NackTrackingSource source = container.select(NackTrackingSource.class).get();

        // Processing is fast, so all messages should complete normally
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(bean.getProcessedCount()).isEqualTo(5));

        assertThat(source.getNacks()).isEmpty();
        assertThat(source.getAcks()).hasSize(5);
    }

    @Test
    public void testInterceptorOnlyAppliesToMatchingChannels() {
        addBeanClass(ChannelFilteringInterceptor.class, FastSubscriberBean.class, NackTrackingSource.class);
        initialize();

        FastSubscriberBean bean = container.select(FastSubscriberBean.class).get();
        NackTrackingSource source = container.select(NackTrackingSource.class).get();

        // The interceptor only applies to "other-channel", not "data",
        // so processing should complete normally
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(bean.getProcessedCount()).isEqualTo(5));

        assertThat(source.getNacks()).isEmpty();
    }

    @ApplicationScoped
    public static class TimeoutInterceptor implements ProcessingDecorator {
        @Override
        public ProcessingInterceptor decorate(MediatorConfiguration configuration) {
            return (processing, message) -> processing.ifNoItem().after(Duration.ofMillis(200)).fail();
        }
    }

    @ApplicationScoped
    public static class ChannelFilteringInterceptor implements ProcessingDecorator {
        @Override
        public ProcessingInterceptor decorate(MediatorConfiguration configuration) {
            if (configuration.getIncoming().contains("other-channel")) {
                return (processing, message) -> processing.ifNoItem().after(Duration.ofMillis(1)).fail();
            }
            return null;
        }
    }

    @ApplicationScoped
    public static class NackTrackingSource {
        private final List<Integer> acks = new CopyOnWriteArrayList<>();
        private final List<Integer> nacks = new CopyOnWriteArrayList<>();
        private final List<Throwable> nackReasons = new CopyOnWriteArrayList<>();

        @Outgoing("data")
        public Flow.Publisher<Message<Integer>> source() {
            return Multi.createFrom().range(0, 5)
                    .map(i -> Message.of(i,
                            () -> {
                                acks.add(i);
                                return CompletableFuture.completedFuture(null);
                            },
                            t -> {
                                nacks.add(i);
                                nackReasons.add(t);
                                return CompletableFuture.completedFuture(null);
                            }));
        }

        public List<Integer> getAcks() {
            return acks;
        }

        public List<Integer> getNacks() {
            return nacks;
        }

        public List<Throwable> getNackReasons() {
            return nackReasons;
        }
    }

    @ApplicationScoped
    public static class SlowSubscriberBean {
        @Incoming("data")
        public Uni<Void> consume(int payload) {
            // Simulate slow processing (longer than 200ms timeout)
            return Uni.createFrom().voidItem()
                    .onItem().delayIt().by(Duration.ofSeconds(2));
        }
    }

    @ApplicationScoped
    public static class FastSubscriberBean {
        private final AtomicInteger processedCount = new AtomicInteger();

        @Incoming("data")
        public CompletionStage<Void> consume(int payload) {
            processedCount.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        }

        public int getProcessedCount() {
            return processedCount.get();
        }
    }
}
