package io.smallrye.reactive.messaging.kafka.health;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.Flow;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.kafka.common.serialization.IntegerSerializer;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;
import io.smallrye.reactive.messaging.kafka.companion.ConsumerTask;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class SinkHealthCheckTest extends KafkaCompanionTestBase {

    private KafkaMapBasedConfig getKafkaSinkConfigForProducingBean() {
        return kafkaConfig("mp.messaging.outgoing.output")
                .put("value.serializer", IntegerSerializer.class.getName());
    }

    @Test
    public void testWithoutOutgoingTopic() {
        MapBasedConfig config = new MapBasedConfig(getKafkaSinkConfigForProducingBean());
        config.put("my.topic", topic);
        runApplication(config, ProducingBean.class);

        await().until(() -> isStarted() && isReady() && isAlive());

        ConsumerTask<String, Integer> records = companion.consumeIntegers().fromTopics(topic, 10, Duration.ofSeconds(10));

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
    public void testWithoutOutgoingTopicReadinessDisabled() {
        MapBasedConfig config = new MapBasedConfig(getKafkaSinkConfigForProducingBean()
                .put("health-readiness-enabled", false));
        config.put("my.topic", topic);
        runApplication(config, ProducingBean.class);

        await().until(() -> isStarted() && isReady() && isAlive());

        ConsumerTask<String, Integer> records = companion.consumeIntegers().fromTopics(topic, 10, Duration.ofSeconds(10));

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

    @Test
    void testWithOutgoingTopicUsingTopicVerification() {
        String outputTopic = UUID.randomUUID().toString();
        MapBasedConfig config = new MapBasedConfig(getKafkaSinkConfigForProducingBean()
                .put("health-topic-verification-enabled", true)
                .put("topic", outputTopic));
        config.put("my.topic", topic);
        runApplication(config, ProducingBean.class);

        await().until(() -> isReady() && !isStarted());

        companion.topics().createAndWait(outputTopic, 1, Duration.ofMinutes(1));
        ConsumerTask<String, Integer> consume = companion.consumeIntegers().fromTopics(topic, 10, Duration.ofSeconds(10));

        await().until(() -> isStarted() && isReady() && isAlive());

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
    void testWithOutgoingTopicUsingTopicVerificationStartupDisabled() {
        String outputTopic = UUID.randomUUID().toString();
        MapBasedConfig config = new MapBasedConfig(getKafkaSinkConfigForProducingBean()
                .put("health-topic-verification-enabled", true)
                .put("health-topic-verification-startup-disabled", true)
                .put("topic", outputTopic));
        config.put("my.topic", topic);
        runApplication(config, ProducingBean.class);

        await().until(() -> isStarted() && isReady() && isAlive());

        companion.topics().createAndWait(outputTopic, 1, Duration.ofMinutes(1));
        ConsumerTask<String, Integer> consume = companion.consumeIntegers().fromTopics(topic, 10, Duration.ofSeconds(10));

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
    void testWithOutgoingTopicUsingTopicVerificationReadinessDisabled() {
        String outputTopic = UUID.randomUUID().toString();
        MapBasedConfig config = new MapBasedConfig(getKafkaSinkConfigForProducingBean()
                .put("health-topic-verification-enabled", true)
                .put("health-topic-verification-startup-disabled", true)
                .put("topic", outputTopic));
        config.put("my.topic", topic);
        runApplication(config, ProducingBean.class);

        await().until(() -> isStarted() && isReady() && isAlive());

        companion.topics().createAndWait(outputTopic, 1, Duration.ofMinutes(1));
        ConsumerTask<String, Integer> consume = companion.consumeIntegers().fromTopics(topic, 10, Duration.ofSeconds(10));

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
        public Flow.Publisher<Integer> source() {
            return Multi.createFrom().range(0, 10);
        }

    }

}
