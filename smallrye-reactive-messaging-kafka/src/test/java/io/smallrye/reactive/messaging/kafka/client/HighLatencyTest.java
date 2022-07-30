package io.smallrye.reactive.messaging.kafka.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import eu.rekawek.toxiproxy.model.ToxicDirection;
import io.smallrye.reactive.messaging.kafka.CountKafkaCdiEvents;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import io.smallrye.reactive.messaging.kafka.TestTags;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionProxyTestBase;
import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;
import io.smallrye.reactive.messaging.kafka.base.UnsatisfiedInstance;
import io.smallrye.reactive.messaging.kafka.impl.KafkaSource;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

@Tag(TestTags.SLOW)
public class HighLatencyTest extends KafkaCompanionProxyTestBase {

    KafkaSource<Integer, String> source;

    @AfterEach
    public void tearDown() {
        source.closeQuietly();
    }

    @BeforeEach
    public void init() {
        topic = companion.topics().createAndWait("test-" + UUID.randomUUID().toString(), 4);
    }

    public KafkaMapBasedConfig newCommonConfigForSource() {
        String randomId = UUID.randomUUID().toString();
        return kafkaConfig().build(
                "group.id", randomId,
                "key.deserializer", StringDeserializer.class.getName(),
                "enable.auto.commit", "false",
                "auto.offset.reset", "earliest",
                "topic", topic,
                "channel-name", topic);
    }

    @Test
    public void testHighLatency() throws InterruptedException, IOException {
        MapBasedConfig config = newCommonConfigForSource()
                .with("value.deserializer", IntegerDeserializer.class.getName())
                .with(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 6000)
                .with(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 100)
                .with("retry", true)
                .with("retry-attempts", 100)
                .with("retry-max-wait", 30);

        KafkaConnectorIncomingConfiguration ic = new KafkaConnectorIncomingConfiguration(config);
        source = new KafkaSource<>(vertx, UUID.randomUUID().toString(), ic, commitHandlerFactories, failureHandlerFactories,
                UnsatisfiedInstance.instance(), CountKafkaCdiEvents.noCdiEvents,
                UnsatisfiedInstance.instance(), -1);
        List<KafkaRecord<?, ?>> messages1 = new ArrayList<>();
        source.getStream().subscribe().with(messages1::add);

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, i), 10);

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages1.size() >= 10);

        toxics().latency("latency", ToxicDirection.UPSTREAM, 6000 + 1000);
        Thread.sleep(6000 + 2000); // session timeout + a bit more just in case.
        toxics().get("latency").remove();

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, 10 + i), 10);

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages1.size() >= 20);
        assertThat(messages1.size()).isGreaterThanOrEqualTo(20);
    }
}
