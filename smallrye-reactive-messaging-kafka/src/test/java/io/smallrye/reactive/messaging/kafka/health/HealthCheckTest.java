package io.smallrye.reactive.messaging.kafka.health;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.UUID;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

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
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;
import io.smallrye.reactive.messaging.kafka.companion.ConsumerTask;
import io.smallrye.reactive.messaging.kafka.companion.ProducerTask;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class HealthCheckTest extends KafkaCompanionTestBase {

    @Test
    public void testHealthOfApplicationWithoutOutgoingTopic() {
        MapBasedConfig config = new MapBasedConfig(getKafkaSinkConfigForProducingBean());
        config.put("my.topic", topic);
        runApplication(config, ProducingBean.class);

        ConsumerTask<String, Integer> records = companion.consumeIntegers().fromTopics(topic, 10, Duration.ofSeconds(10));

        await().until(this::isStarted);
        await().until(this::isReady);
        await().until(this::isAlive);

        await().until(() -> records.count() == 10);
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
        String outputTopic = UUID.randomUUID().toString();
        companion.topics().createAndWait(outputTopic, 1, Duration.ofMinutes(1));
        MapBasedConfig config = new MapBasedConfig(getKafkaSinkConfigForProducingBean()
                .put("health-topic-verification-enabled", true)
                .put("topic", outputTopic));
        config.put("my.topic", topic);
        runApplication(config, ProducingBean.class);

        ConsumerTask<String, Integer> consume = companion.consumeIntegers().fromTopics(topic, 10, Duration.ofSeconds(10));

        await().until(this::isStarted);
        await().until(this::isReady);
        await().until(this::isAlive);

        await().until(() -> consume.count() == 10);
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
        MapBasedConfig config = new MapBasedConfig(getKafkaSinkConfigForProducingBean()
                .put("health-readiness-enabled", false));
        config.put("my.topic", topic);
        runApplication(config, ProducingBean.class);

        ConsumerTask<String, Integer> records = companion.consumeIntegers().fromTopics(topic, 10, Duration.ofSeconds(10));

        await().until(this::isStarted);
        await().until(this::isReady);
        await().until(this::isAlive);

        await().until(() -> records.count() == 10);
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

    private KafkaMapBasedConfig getKafkaSinkConfigForProducingBean() {
        return kafkaConfig("mp.messaging.outgoing.output")
                .put("value.serializer", IntegerSerializer.class.getName());
    }

    private KafkaMapBasedConfig getKafkaSourceConfig(String topic) {
        return kafkaConfig("mp.messaging.incoming.input")
                .put("value.deserializer", IntegerDeserializer.class.getName())
                .put("topic", topic)
                .put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    @Test
    public void testHealthOfApplicationWithChannelUsingTopicVerification() {
        KafkaMapBasedConfig config = getKafkaSourceConfig(topic)
                .put("health-readiness-topic-verification", true);
        LazyConsumingBean bean = runApplication(config, LazyConsumingBean.class);

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, "key", i), 10);

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
        KafkaMapBasedConfig config = getKafkaSourceConfig(topic);
        LazyConsumingBean bean = runApplication(config, LazyConsumingBean.class);

        ProducerTask produced = companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, "key", i), 10);

        await().until(this::isStarted);
        await().until(this::isReady);
        await().until(this::isAlive);

        produced.awaitCompletion(Duration.ofMinutes(1));

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
