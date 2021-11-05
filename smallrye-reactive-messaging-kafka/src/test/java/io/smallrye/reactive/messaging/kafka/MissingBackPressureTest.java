package io.smallrye.reactive.messaging.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;
import io.smallrye.reactive.messaging.kafka.base.KafkaTestBase;

public class MissingBackPressureTest extends KafkaTestBase {

    @Test
    @Disabled("to be investigated - fail on CI")
    public void testWithInterval() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger expected = new AtomicInteger(0);
        usage.consumeStrings(topic, 10, 10, TimeUnit.SECONDS,
                latch::countDown,
                (k, v) -> expected.getAndIncrement());

        runApplication(myKafkaSinkConfig(), MyOutgoingBean.class);

        assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
        assertThat(expected).hasValueGreaterThanOrEqualTo(10);
    }

    public KafkaMapBasedConfig myKafkaSinkConfig() {
        return kafkaConfig("mp.messaging.outgoing.temperature-values")
                .put("value.serializer", StringSerializer.class.getName())
                .put("topic", topic)
                .put("waitForWriteCompletion", false);
    }

    @Test
    public void testWithEmitter() throws InterruptedException {
        List<Map.Entry<String, String>> received = new CopyOnWriteArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger expected = new AtomicInteger(0);
        usage.consumeStrings(topic, 10, 10, TimeUnit.SECONDS,
                latch::countDown,
                (k, v) -> {
                    received.add(entry(k, v));
                    expected.getAndIncrement();
                });

        runApplication(myKafkaSinkConfig(), MyEmitterBean.class);

        MyEmitterBean bean = get(MyEmitterBean.class);
        bean.run();

        assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();

        bean.stop();
        assertThat(expected).hasValueGreaterThanOrEqualTo(10);

        // Check that the 10 first value matches the emitted values.
        List<KafkaRecord<String, String>> messages = bean.emitted();
        Iterator<Map.Entry<String, String>> iterator = received.iterator();
        for (int i = 0; i < 10; i++) {
            KafkaRecord<String, String> message = messages.get(i);
            Map.Entry<String, String> entry = iterator.next();
            assertThat(entry.getKey()).isEqualTo(message.getKey());
            assertThat(entry.getValue()).isEqualTo(message.getPayload());
        }
    }

    @ApplicationScoped
    public static class MyOutgoingBean {

        private final Random random = new Random();

        @Outgoing("temperature-values")
        public Multi<KafkaRecord<String, String>> generate() {

            return Multi.createFrom().ticks().every(Duration.ofMillis(10))
                    .map(tick -> {
                        double temperature = BigDecimal.valueOf(random.nextGaussian() * 15)
                                .setScale(1, RoundingMode.HALF_UP)
                                .doubleValue();
                        return KafkaRecord.of("1", Instant.now().toEpochMilli() + ";" + temperature);
                    });
        }
    }

    @ApplicationScoped
    public static class MyEmitterBean {

        @Inject
        @Channel("temperature-values")
        private Emitter<String> emitter;

        private volatile boolean stop = false;

        private final Random random = new Random();
        private final List<KafkaRecord<String, String>> emitted = new CopyOnWriteArrayList<>();

        public void stop() {
            this.stop = true;
        }

        public List<KafkaRecord<String, String>> emitted() {
            return emitted;
        }

        public void run() {
            new Thread(() -> {
                while (!stop) {
                    double temperature = BigDecimal.valueOf(random.nextGaussian() * 15)
                            .setScale(1, RoundingMode.HALF_UP)
                            .doubleValue();
                    KafkaRecord<String, String> message = KafkaRecord.of("1",
                            Instant.now().toEpochMilli() + ";" + temperature);
                    emitter.send(message);
                    emitted.add(message);
                    nap();
                }
            }).start();
        }

        private void nap() {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

}
