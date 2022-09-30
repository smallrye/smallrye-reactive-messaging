package io.smallrye.reactive.messaging.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;
import io.smallrye.reactive.messaging.kafka.companion.ConsumerTask;

public class MissingBackPressureTest extends KafkaCompanionTestBase {

    @Test
    // to be investigated - fail on CI
    @Tag(TestTags.FLAKY)
    public void testWithInterval() {
        ConsumerTask<String, String> consume = companion.consumeStrings().fromTopics(topic, 10, Duration.ofSeconds(10));

        runApplication(myKafkaSinkConfig(), MyOutgoingBean.class);

        assertThat(consume.awaitCompletion(Duration.ofMinutes(1)).count()).isGreaterThanOrEqualTo(10);
    }

    public KafkaMapBasedConfig myKafkaSinkConfig() {
        return kafkaConfig("mp.messaging.outgoing.temperature-values")
                .put("value.serializer", StringSerializer.class.getName())
                .put("topic", topic)
                .put("waitForWriteCompletion", false);
    }

    @Test
    public void testWithEmitter() {
        ConsumerTask<String, String> consume = companion.consumeStrings().fromTopics(topic, 10, Duration.ofSeconds(10));

        runApplication(myKafkaSinkConfig(), MyEmitterBean.class);

        MyEmitterBean bean = get(MyEmitterBean.class);
        bean.run();

        consume.awaitCompletion(Duration.ofMinutes(1));

        bean.stop();
        assertThat(consume.count()).isGreaterThanOrEqualTo(10);

        // Check that the 10 first value matches the emitted values.
        List<KafkaRecord<String, String>> messages = bean.emitted();
        Iterator<ConsumerRecord<String, String>> iterator = consume.getRecords().iterator();
        for (int i = 0; i < 10; i++) {
            KafkaRecord<String, String> message = messages.get(i);
            ConsumerRecord<String, String> record = iterator.next();
            assertThat(record.key()).isEqualTo(message.getKey());
            assertThat(record.value()).isEqualTo(message.getPayload());
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
