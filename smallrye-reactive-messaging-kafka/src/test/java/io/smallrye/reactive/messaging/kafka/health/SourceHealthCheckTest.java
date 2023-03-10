package io.smallrye.reactive.messaging.kafka.health;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;
import io.smallrye.reactive.messaging.kafka.companion.ProducerTask;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class SourceHealthCheckTest extends KafkaCompanionTestBase {

    private KafkaMapBasedConfig getKafkaSourceConfig(String topic) {
        return kafkaConfig("mp.messaging.incoming.input")
                .put("value.deserializer", IntegerDeserializer.class.getName())
                .put("topic", topic)
                .put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    @Test
    public void testWithIncomingChannel() {
        KafkaMapBasedConfig config = getKafkaSourceConfig(topic);
        LazyConsumingBean bean = runApplication(config, LazyConsumingBean.class);

        ProducerTask produced = companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, "key", i), 10);

        await().until(() -> isStarted() && isReady() && isAlive());

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

    @Test
    public void testWithReadinessDisabled() {
        KafkaMapBasedConfig config = getKafkaSourceConfig(topic)
                .with("health-readiness-enabled", false);
        LazyConsumingBean bean = runApplication(config, LazyConsumingBean.class);

        ProducerTask produced = companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, "key", i), 10);

        await().until(() -> isStarted() && isReady() && isAlive());

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
        assertThat(readiness.getChannels()).hasSize(0);
    }

    @Test
    public void testWithMultipleSubscribedTopic() {
        String[] topics = { topic, topic + "-1", topic + "-2" };
        MapBasedConfig config = getKafkaSourceConfig(topic)
                .with("topics", String.join(",", topics))
                .without("mp.messaging.incoming.input.topic");
        LazyConsumingBean bean = runApplication(config, LazyConsumingBean.class);

        ProducerTask produced = companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topics[i % 3], "key", i),
                10);

        await().until(() -> isStarted() && isReady() && isAlive());

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

    @Test
    public void testWithTopicVerification() {
        KafkaMapBasedConfig config = getKafkaSourceConfig(topic)
                .put("health-readiness-topic-verification", true);
        LazyConsumingBean bean = runApplication(config, LazyConsumingBean.class);

        await().until(() -> !isStarted() && isReady());

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, "key", i), 10);

        await().until(() -> isStarted() && isReady() && isAlive());

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
    public void testWithTopicVerificationMultipleTopics() {
        String[] topics = { topic, topic + "-1", topic + "-2" };
        MapBasedConfig config = getKafkaSourceConfig(topic)
                .with("health-topic-verification-enabled", true)
                .with("topics", String.join(",", topics))
                .without("mp.messaging.incoming.input.topic");
        LazyConsumingBean bean = runApplication(config, LazyConsumingBean.class);

        await().until(() -> !isStarted() && isReady());

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topics[i % 3], "key", i), 10);

        await().until(() -> isStarted() && isReady() && isAlive());

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
    public void testWithTopicVerificationTopicsPattern() {
        String[] topics = { topic, topic + "-1", topic + "-2" };

        MapBasedConfig config = getKafkaSourceConfig(topic)
                .with("health-topic-verification-enabled", true)
                .with("topic", topic + ".*")
                .with("pattern", true);
        LazyConsumingBean bean = runApplication(config, LazyConsumingBean.class);

        await().until(() -> !isStarted() && isReady());

        for (String topic : topics) {
            companion.topics().createAndWait(topic, 1);
        }
        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topics[i % 3], "key", i), 10);

        await().until(() -> isStarted() && isReady() && isAlive());

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
    public void testWithTopicVerificationStartupDisabled() {
        KafkaMapBasedConfig config = getKafkaSourceConfig(topic)
                .with("health-topic-verification-enabled", true)
                .with("health-topic-verification-startup-disabled", true);
        LazyConsumingBean bean = runApplication(config, LazyConsumingBean.class);

        await().until(() -> isStarted() && isReady() && isAlive());

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, "key", i), 10);

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
