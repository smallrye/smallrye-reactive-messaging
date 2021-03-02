package io.smallrye.reactive.messaging.kafka.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import eu.rekawek.toxiproxy.model.ToxicDirection;
import io.smallrye.reactive.messaging.kafka.CountKafkaCdiEvents;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import io.smallrye.reactive.messaging.kafka.base.KafkaBrokerExtension;
import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;
import io.smallrye.reactive.messaging.kafka.base.TopicHelpers;
import io.smallrye.reactive.messaging.kafka.base.UnsatisfiedInstance;
import io.smallrye.reactive.messaging.kafka.impl.KafkaSource;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

@Disabled("too long")
public class HighLatencyTest extends ClientTestBase {

    private KafkaSource<String, Integer> source;

    @AfterEach
    public void tearDown() {
        source.closeQuietly();
    }

    @BeforeEach
    public void init() {
        topic = TopicHelpers.createNewTopic("test-" + UUID.randomUUID().toString(), partitions);
        resetMessages();
    }

    public KafkaMapBasedConfig newCommonConfigForSource() {
        String randomId = UUID.randomUUID().toString();
        return KafkaMapBasedConfig.builder().put(
                "bootstrap.servers", getBootstrapServers(),
                "group.id", randomId,
                "key.deserializer", StringDeserializer.class.getName(),
                "enable.auto.commit", "false",
                "auto.offset.reset", "earliest",
                "tracing-enabled", false,
                "topic", topic,
                "graceful-shutdown", false,
                "channel-name", topic).build();
    }

    @Test
    public void testHighLatency() throws InterruptedException, IOException {
        MapBasedConfig config = newCommonConfigForSource()
                .with("bootstrap.servers", getBootstrapServers())
                .with("value.deserializer", IntegerDeserializer.class.getName())
                .with(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 6000)
                .with(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 100)
                .with("retry", true)
                .with("retry-attempts", 100)
                .with("retry-max-wait", 30);

        usage.setBootstrapServers(KafkaBrokerExtension.getBootstrapServers());

        KafkaConnectorIncomingConfiguration ic = new KafkaConnectorIncomingConfiguration(config);
        source = new KafkaSource<>(vertx, UUID.randomUUID().toString(), ic,
                UnsatisfiedInstance.instance(), CountKafkaCdiEvents.noCdiEvents,
                UnsatisfiedInstance.instance(), -1);
        List<KafkaRecord<?, ?>> messages1 = new ArrayList<>();
        source.getStream().subscribe().with(messages1::add);

        AtomicInteger counter = new AtomicInteger();

        usage.produceIntegers(10, null,
                () -> new ProducerRecord<>(topic, counter.getAndIncrement()));

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages1.size() >= 10);

        KafkaBrokerExtension.getProxy().toxics().latency("latency", ToxicDirection.DOWNSTREAM, 6000 + 1000);
        Thread.sleep(6000 + 2000); // session timeout + a bit more just in case.
        KafkaBrokerExtension.getProxy().toxics().get("latency").remove();

        new Thread(() -> usage.produceIntegers(10, null,
                () -> new ProducerRecord<>(topic, counter.getAndIncrement()))).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages1.size() >= 20);
        assertThat(messages1.size()).isGreaterThanOrEqualTo(20);
    }
}
