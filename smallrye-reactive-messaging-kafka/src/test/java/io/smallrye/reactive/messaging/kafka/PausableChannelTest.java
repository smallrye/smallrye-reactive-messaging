package io.smallrye.reactive.messaging.kafka;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.LongAdder;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.ChannelRegistry;
import io.smallrye.reactive.messaging.PausableChannel;
import io.smallrye.reactive.messaging.annotations.Blocking;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;

public class PausableChannelTest extends KafkaCompanionTestBase {

    public static final int COUNT = 100;

    private KafkaMapBasedConfig commonConfig() {
        return kafkaConfig("mp.messaging.outgoing.out")
                .with("topic", topic)
                .put("key.serializer", StringSerializer.class.getName())
                .put("value.serializer", IntegerSerializer.class.getName())
                .withPrefix("mp.messaging.incoming.data")
                .put("topic", topic)
                .put("cloud-events", false)
                .put("commit-strategy", "throttled")
                .put("auto.offset.reset", "earliest")
                .put("key.deserializer", StringDeserializer.class.getName())
                .put("value.deserializer", IntegerDeserializer.class.getName());
    }

    @Test
    public void testPausableChannelInitiallyPaused() {
        addBeans(MyMessageProducer.class);
        ConsumerApp application = runApplication(commonConfig()
                .with("pausable", true)
                .with("initially-paused", true), ConsumerApp.class);
        ChannelRegistry pausableChannels = get(ChannelRegistry.class);
        PausableChannel pauser = pausableChannels.getPausable("data");

        long firstStep = COUNT / 10;
        long secondStep = COUNT / 5;
        long finalStep = COUNT;
        await().pollDelay(1, SECONDS).untilAsserted(() -> assertThat(application.getCount()).isEqualTo(0L));
        assertThat(pauser.isPaused()).isTrue();
        pauser.resume();
        await().untilAsserted(() -> assertThat(application.getCount()).isGreaterThan(firstStep));
        pauser.pause();
        await().untilAsserted(() -> assertThat(application.getCount()).isBetween(firstStep, finalStep));
        assertThat(pauser.isPaused()).isTrue();
        pauser.resume();
        await().untilAsserted(() -> assertThat(application.getCount()).isGreaterThan(secondStep));
        pauser.pause();
        await().untilAsserted(() -> assertThat(application.getCount()).isBetween(secondStep, finalStep));
        assertThat(pauser.isPaused()).isTrue();
        pauser.resume();
        await().untilAsserted(() -> assertThat(application.getCount()).isEqualTo(COUNT));
    }

    @Test
    public void testPausableChannel() {
        addBeans(MyMessageProducer.class);
        ConsumerApp application = runApplication(commonConfig()
                .with("pausable", true), ConsumerApp.class);
        ChannelRegistry pausableChannels = get(ChannelRegistry.class);
        PausableChannel pauser = pausableChannels.getPausable("data");

        long firstStep = COUNT / 10;
        long secondStep = COUNT / 5;
        long finalStep = COUNT;
        await().untilAsserted(() -> assertThat(application.getCount()).isGreaterThan(firstStep));
        assertThat(pauser.isPaused()).isFalse();
        pauser.pause();
        await().untilAsserted(() -> assertThat(application.getCount()).isBetween(firstStep, finalStep));
        pauser.resume();
        await().untilAsserted(() -> assertThat(application.getCount()).isGreaterThan(secondStep));
        pauser.pause();
        await().untilAsserted(() -> assertThat(application.getCount()).isBetween(secondStep, finalStep));
        pauser.resume();
        await().untilAsserted(() -> assertThat(application.getCount()).isEqualTo(COUNT));
    }

    @Test
    public void testPausableChannelWithPauser() {
        addBeans(MyMessageProducer.class);
        ConsumerAppWithPauser application = runApplication(commonConfig()
                .with("pausable", true), ConsumerAppWithPauser.class);
        ChannelRegistry pausableChannels = get(ChannelRegistry.class);
        PausableChannel pauser = pausableChannels.getPausable("data");

        assertThat(pauser.isPaused()).isFalse();
        await().untilAsserted(() -> {
            if (pauser.isPaused()) {
                pauser.resume();
            }
            assertThat(application.getCount()).isEqualTo(COUNT);
        });
        assertThat(application.getPaused()).isEqualTo(5);
    }

    @ApplicationScoped
    public static class ConsumerApp {

        LongAdder count = new LongAdder();
        List<Integer> list = new CopyOnWriteArrayList<>();

        @Incoming("data")
        @Blocking
        public void consume(Integer message) throws InterruptedException {
            list.add(message);
            count.increment();
            Thread.sleep(50);
        }

        public List<Integer> get() {
            return list;
        }

        public long getCount() {
            return count.longValue();
        }
    }

    @ApplicationScoped
    public static class ConsumerAppWithPauser {

        @Inject
        ChannelRegistry registry;

        LongAdder count = new LongAdder();
        LongAdder paused = new LongAdder();
        List<Integer> list = new CopyOnWriteArrayList<>();

        @Incoming("data")
        @Blocking
        public void consume(Integer message) throws InterruptedException {
            list.add(message);
            count.increment();
            if (count.longValue() % 20 == 0) {
                PausableChannel data = registry.getPausable("data");
                data.pause();
                paused.increment();
            }
        }

        public List<Integer> get() {
            return list;
        }

        public long getCount() {
            return count.longValue();
        }

        public long getPaused() {
            return paused.longValue();
        }
    }

    @ApplicationScoped
    public static class MyMessageProducer {

        List<Integer> produced = new CopyOnWriteArrayList<>();

        @Outgoing("out")
        public Multi<Message<Integer>> generate() {
            return Multi.createFrom().range(0, COUNT)
                    .map(i -> Message.of(i, () -> {
                        produced.add(i);
                        return CompletableFuture.completedFuture(null);
                    }));
        }
    }

}
