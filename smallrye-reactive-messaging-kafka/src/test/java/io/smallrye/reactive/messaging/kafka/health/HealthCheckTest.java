package io.smallrye.reactive.messaging.kafka.health;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.*;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;
import io.smallrye.reactive.messaging.kafka.base.KafkaTestBase;

public class HealthCheckTest extends KafkaTestBase {

    @Test
    public void testHealthOfApplicationWithoutOutgoingTopic() {
        KafkaMapBasedConfig config = getKafkaSinkConfigForProducingBean().build();
        config.put("my.topic", topic);
        runApplication(config, ProducingBean.class);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger expected = new AtomicInteger(0);
        usage.consumeIntegers(topic, 10, 10, TimeUnit.SECONDS,
                latch::countDown,
                (k, v) -> expected.getAndIncrement());

        await().until(this::isStarted);
        await().until(this::isReady);
        await().until(this::isAlive);

        await().until(() -> expected.get() == 10);
        HealthReport startup = getHealth().getStartup();
        HealthReport liveness = getHealth().getLiveness();
        HealthReport readiness = getHealth().getReadiness();

        assertThat(startup.isOk()).isTrue();
        assertThat(liveness.isOk()).isTrue();
        assertThat(readiness.isOk()).isTrue();
        assertThat(startup.getChannels()).hasSize(1);
        assertThat(liveness.getChannels()).hasSize(1);
        assertThat(readiness.getChannels()).hasSize(1);
        assertThat(liveness.getChannels().get(0).getChannel()).isEqualTo("output");
    }

    @Test
    void testHealthOfApplicationWithOutgoingTopicUsingTopicVerification() {
        createTopic("output", 1);
        KafkaMapBasedConfig config = getKafkaSinkConfigForProducingBean()
                .put("health-readiness-topic-verification", true)
                .build();
        config.put("my.topic", topic);
        runApplication(config, ProducingBean.class);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger expected = new AtomicInteger(0);
        usage.consumeIntegers(topic, 10, 10, TimeUnit.SECONDS,
                latch::countDown,
                (k, v) -> expected.getAndIncrement());

        await().until(this::isStarted);
        await().until(this::isReady);
        await().until(this::isAlive);

        await().until(() -> expected.get() == 10);
        HealthReport startup = getHealth().getStartup();
        HealthReport liveness = getHealth().getLiveness();
        HealthReport readiness = getHealth().getReadiness();

        assertThat(startup.isOk()).isTrue();
        assertThat(liveness.isOk()).isTrue();
        assertThat(readiness.isOk()).isTrue();
        assertThat(startup.getChannels()).hasSize(1);
        assertThat(liveness.getChannels()).hasSize(1);
        assertThat(readiness.getChannels()).hasSize(1);
        assertThat(liveness.getChannels().get(0).getChannel()).isEqualTo("output");
    }

    @Test
    public void testHealthOfApplicationWithoutOutgoingTopicReadinessDisabled() {
        KafkaMapBasedConfig config = getKafkaSinkConfigForProducingBean()
                .put("health-readiness-enabled", false)
                .build();
        config.put("my.topic", topic);
        runApplication(config, ProducingBean.class);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger expected = new AtomicInteger(0);
        usage.consumeIntegers(topic, 10, 10, TimeUnit.SECONDS,
                latch::countDown,
                (k, v) -> expected.getAndIncrement());

        await().until(this::isStarted);
        await().until(this::isReady);
        await().until(this::isAlive);

        await().until(() -> expected.get() == 10);
        HealthReport startup = getHealth().getStartup();
        HealthReport liveness = getHealth().getLiveness();
        HealthReport readiness = getHealth().getReadiness();

        assertThat(startup.isOk()).isTrue();
        assertThat(liveness.isOk()).isTrue();
        assertThat(readiness.isOk()).isTrue();
        assertThat(startup.getChannels()).hasSize(1);
        assertThat(liveness.getChannels()).hasSize(1);
        assertThat(readiness.getChannels()).hasSize(0);
        assertThat(liveness.getChannels().get(0).getChannel()).isEqualTo("output");
    }

    private KafkaMapBasedConfig.Builder getKafkaSinkConfigForProducingBean() {
        return KafkaMapBasedConfig.builder("mp.messaging.outgoing.output")
                .put("value.serializer", IntegerSerializer.class.getName());
    }

    private KafkaMapBasedConfig.Builder getKafkaSourceConfig(String topic) {
        return KafkaMapBasedConfig.builder("mp.messaging.incoming.input")
                .put("value.deserializer", IntegerDeserializer.class.getName())
                .put("topic", topic)
                .put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    @Test
    public void testHealthOfApplicationWithChannelUsingTopicVerification() {
        KafkaMapBasedConfig config = getKafkaSourceConfig(topic)
                .put("health-readiness-topic-verification", true)
                .build();
        LazyConsumingBean bean = runApplication(config, LazyConsumingBean.class);

        AtomicInteger expected = new AtomicInteger(0);
        usage.produceIntegers(10, null,
                () -> new ProducerRecord<>(topic, "key", expected.getAndIncrement()));

        await().until(this::isStarted);
        await().until(this::isReady);
        await().until(this::isAlive);
        // before subscription to channel

        Multi<Integer> channel = bean.getChannel();
        channel
                .select().first(10)
                .collect().asList()
                .await().atMost(Duration.ofSeconds(10));

        // after subscription to channel

        HealthReport startup = getHealth().getStartup();
        HealthReport liveness = getHealth().getLiveness();
        HealthReport readiness = getHealth().getReadiness();

        assertThat(startup.isOk()).isTrue();
        assertThat(liveness.isOk()).isTrue();
        assertThat(readiness.isOk()).isTrue();
        assertThat(startup.getChannels()).hasSize(1);
        assertThat(liveness.getChannels()).hasSize(1);
        assertThat(readiness.getChannels()).hasSize(1);
    }

    @Test
    public void testHealthOfApplicationWithChannel() {
        KafkaMapBasedConfig config = getKafkaSourceConfig(topic).build();
        LazyConsumingBean bean = runApplication(config, LazyConsumingBean.class);

        AtomicInteger expected = new AtomicInteger(0);
        usage.produceIntegers(10, null,
                () -> new ProducerRecord<>(topic, "key", expected.getAndIncrement()));

        await().until(this::isStarted);
        await().until(this::isReady);
        await().until(this::isAlive);

        Multi<Integer> channel = bean.getChannel();
        channel
                .select().first(10)
                .collect().asList()
                .await().atMost(Duration.ofSeconds(10));

        HealthReport startup = getHealth().getStartup();
        HealthReport liveness = getHealth().getLiveness();
        HealthReport readiness = getHealth().getReadiness();

        assertThat(startup.isOk()).isTrue();
        assertThat(liveness.isOk()).isTrue();
        assertThat(readiness.isOk()).isTrue();
        assertThat(startup.getChannels()).hasSize(1);
        assertThat(liveness.getChannels()).hasSize(1);
        assertThat(readiness.getChannels()).hasSize(1);
    }

    @ApplicationScoped
    public static class ProducingBean {

        @Inject
        @ConfigProperty(name = "my.topic")
        String topic;

        @Incoming("data")
        @Outgoing("output")
        @Acknowledgment(Acknowledgment.Strategy.MANUAL)
        public Message<Integer> process(Message<Integer> input) {
            return Message.of(input.getPayload() + 1, input::ack)
                    .addMetadata(OutgoingKafkaRecordMetadata.builder().withTopic(topic).build());
        }

        @Outgoing("data")
        public Publisher<Integer> source() {
            return Multi.createFrom().range(0, 10);
        }

    }

    @ApplicationScoped
    public static class LazyConsumingBean {

        @Inject
        @Channel("input")
        Multi<Integer> channel;

        public Multi<Integer> getChannel() {
            return channel;
        }
    }

}
