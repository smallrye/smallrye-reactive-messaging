package io.smallrye.reactive.messaging.kafka.health;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.apache.kafka.common.serialization.IntegerSerializer;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.kafka.OutgoingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;
import io.smallrye.reactive.messaging.kafka.base.KafkaTestBase;

public class HealthCheckTest extends KafkaTestBase {

    @Test
    public void testHealthOfApplicationWithoutOutgoingTopic() {
        KafkaMapBasedConfig config = getKafkaSinkConfigForProducingBean();
        config.put("my.topic", topic);
        runApplication(config, ProducingBean.class);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger expected = new AtomicInteger(0);
        usage.consumeIntegers(topic, 10, 10, TimeUnit.SECONDS,
                latch::countDown,
                (k, v) -> expected.getAndIncrement());

        await().until(this::isReady);
        await().until(this::isAlive);

        await().until(() -> expected.get() == 10);
        HealthReport liveness = getHealth().getLiveness();
        HealthReport readiness = getHealth().getReadiness();

        assertThat(liveness.isOk()).isTrue();
        assertThat(readiness.isOk()).isTrue();
        assertThat(liveness.getChannels()).hasSize(1);
        assertThat(readiness.getChannels()).hasSize(0);
        assertThat(liveness.getChannels().get(0).getChannel()).isEqualTo("output");
    }

    @Test
    public void testHealthOfApplicationWithoutOutgoingTopicUsingAdminCheck() {
        KafkaMapBasedConfig config = getKafkaSinkConfigForProducingBean();
        config.put("my.topic", topic);
        runApplication(config, ProducingBean.class);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger expected = new AtomicInteger(0);
        usage.consumeIntegers(topic, 10, 10, TimeUnit.SECONDS,
                latch::countDown,
                (k, v) -> expected.getAndIncrement());

        await().until(this::isReady);
        await().until(this::isAlive);

        await().until(() -> expected.get() == 10);
        HealthReport liveness = getHealth().getLiveness();
        HealthReport readiness = getHealth().getReadiness();

        assertThat(liveness.isOk()).isTrue();
        assertThat(readiness.isOk()).isTrue();
        assertThat(liveness.getChannels()).hasSize(1);
        assertThat(readiness.getChannels()).hasSize(0);
        assertThat(liveness.getChannels().get(0).getChannel()).isEqualTo("output");
    }

    private KafkaMapBasedConfig getKafkaSinkConfigForProducingBean() {
        KafkaMapBasedConfig.Builder builder = KafkaMapBasedConfig.builder("mp.messaging.outgoing.output")
                .put("value.serializer", IntegerSerializer.class.getName())
                // Disabling readiness
                .put("health-readiness-enabled", false);
        return builder.build();
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

}
