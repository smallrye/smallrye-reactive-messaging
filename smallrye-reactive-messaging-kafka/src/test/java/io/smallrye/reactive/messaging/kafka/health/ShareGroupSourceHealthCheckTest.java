package io.smallrye.reactive.messaging.kafka.health;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;
import io.smallrye.reactive.messaging.kafka.impl.KafkaShareGroupSource;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class ShareGroupSourceHealthCheckTest extends KafkaCompanionTestBase {

    KafkaMapBasedConfig config;
    private String groupId;
    private String clientId;

    @BeforeEach
    void setUp(TestInfo testInfo) {
        String methodName = testInfo.getTestMethod().get().getName();
        groupId = "group-" + methodName + "-" + UUID.randomUUID();
        clientId = "client-" + methodName + "-" + UUID.randomUUID();
        config = kafkaConfig("mp.messaging.incoming.input")
                .with("group.id", groupId)
                .with("client.id", clientId)
                .put("value.deserializer", IntegerDeserializer.class.getName())
                .put("topic", topic)
                .put("share-group", true);
    }

    @Test
    public void testShareGroupHealthWithIncomingChannel() {
        companion.topics().createAndWait(topic, 1);
        ShareGroupLazyConsumingBean bean = runApplication(config, ShareGroupLazyConsumingBean.class);

        List<Integer> received = new CopyOnWriteArrayList<>();
        Multi<Integer> channel = bean.getChannel();
        channel.subscribe().with(received::add);

        await().until(() -> isStarted() && isReady() && isAlive());

        companion.consumerGroups().waitForShareGroupAssignment(groupId)
                .await().atMost(Duration.ofSeconds(20));

        await().until(() -> isStarted() && isReady() && isAlive());

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, "key", i), 10);

        await().until(() -> received.size() >= 10);
        assertThat(received).hasSize(10);

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
    public void testShareGroupHealthWithTopicVerification() {
        companion.topics().createAndWait(topic, 1);
        ShareGroupLazyConsumingBean bean = runApplication(config
                .with("health-readiness-topic-verification", true), ShareGroupLazyConsumingBean.class);

        await().until(() -> isStarted() && isReady());

        List<Integer> received = new CopyOnWriteArrayList<>();
        Multi<Integer> channel = bean.getChannel();
        channel.subscribe().with(received::add);

        await().until(() -> isStarted() && isReady() && isAlive());

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, "key", i), 10);

        await().until(() -> received.size() >= 10);
        assertThat(received).hasSize(10);

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
    public void testShareGroupHealthWithTopicVerificationStartupDisabled() {
        companion.topics().createAndWait(topic, 1);
        ShareGroupLazyConsumingBean bean = runApplication(config
                .with("health-topic-verification-enabled", true)
                .with("health-topic-verification-startup-disabled", true),
                ShareGroupLazyConsumingBean.class);

        List<Integer> received = new CopyOnWriteArrayList<>();
        Multi<Integer> channel = bean.getChannel();
        channel.subscribe().with(received::add);

        await().until(() -> isStarted() && isReady() && isAlive());

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, "key", i), 10);

        await().until(() -> received.size() >= 10);
        assertThat(received).hasSize(10);

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
    public void testShareGroupLivenessReportsFailure() {
        MapBasedConfig config = kafkaConfig().build(
                "group.id", UUID.randomUUID().toString(),
                "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
                "value.deserializer", IntegerDeserializer.class.getName(),
                "tracing-enabled", false,
                "topic", topic,
                "graceful-shutdown", false,
                "share-group", true,
                "channel-name", topic);
        KafkaConnectorIncomingConfiguration ic = new KafkaConnectorIncomingConfiguration(config);
        KafkaShareGroupSource<String, Integer> source = createShareGroupSource(UUID.randomUUID().toString(), config);

        try {
            // Initially liveness should be OK (no failures)
            HealthReport.HealthReportBuilder builder = HealthReport.builder();
            source.isAlive(builder);
            HealthReport report = builder.build();
            assertThat(report.isOk()).isTrue();

            // Report a failure
            source.reportFailure(new RuntimeException("test failure"), false);

            // Now liveness should report NOT OK
            HealthReport.HealthReportBuilder failedBuilder = HealthReport.builder();
            source.isAlive(failedBuilder);
            HealthReport failedReport = failedBuilder.build();
            assertThat(failedReport.isOk()).isFalse();
            assertThat(failedReport.getChannels()).hasSize(1);
            assertThat(failedReport.getChannels().get(0).getMessage()).contains("test failure");
        } finally {
            source.closeQuietly();
        }
    }

    @ApplicationScoped
    public static class ShareGroupLazyConsumingBean {

        @Inject
        @Channel("input")
        Multi<Integer> channel;

        public Multi<Integer> getChannel() {
            return channel;
        }
    }
}
