package io.smallrye.reactive.messaging.kafka.fault;

import static io.smallrye.reactive.messaging.kafka.KafkaConnector.CONNECTOR_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.enterprise.context.ApplicationScoped;

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import io.smallrye.reactive.messaging.kafka.OutgoingKafkaRecord;
import io.smallrye.reactive.messaging.kafka.OutgoingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;
import io.smallrye.reactive.messaging.kafka.base.KafkaTestBase;

public class KafkaNackWithMetadataTest extends KafkaTestBase {
    @Test
    public void test() {
        runApplication(kafkaConfig(), MyApp.class);

        MyApp bean = get(MyApp.class);

        await().until(() -> bean.received().size() > 0);
        KafkaRecord<String, String> record = bean.received().get(0);

        assertThat(record.getKey()).isEqualTo("my-failed-msg");
        assertThat(record.getPayload()).isEqualTo("my-value");
        assertThat(record.getTopic()).isEqualTo(topic + "-dlt");

        // 2 headers added in MyApp + the `dead-letter-*` headers
        assertThat(record.getHeaders()).hasSizeGreaterThan(2);
        assertThat(record.getHeaders()).anySatisfy(header -> {
            assertThat(header.key()).isEqualTo("my-header-key");
            assertThat(new String(header.value(), StandardCharsets.UTF_8)).isEqualTo("my-header-value");
        });
        assertThat(record.getHeaders()).anySatisfy(header -> {
            assertThat(header.key()).isEqualTo("my-failed-header-key");
            assertThat(new String(header.value(), StandardCharsets.UTF_8)).isEqualTo("my-failed-header-value");
        });
    }

    private KafkaMapBasedConfig kafkaConfig() {
        KafkaMapBasedConfig.Builder builder = KafkaMapBasedConfig.builder();

        builder.put("mp.messaging.outgoing.main-producer.connector", CONNECTOR_NAME);
        builder.put("mp.messaging.outgoing.main-producer.bootstrap.servers", getBootstrapServers());
        builder.put("mp.messaging.outgoing.main-producer.topic", topic);
        builder.put("mp.messaging.outgoing.main-producer.key.serializer", StringSerializer.class.getName());
        builder.put("mp.messaging.outgoing.main-producer.value.serializer", StringSerializer.class.getName());

        builder.put("mp.messaging.incoming.main-consumer.connector", CONNECTOR_NAME);
        builder.put("mp.messaging.incoming.main-consumer.bootstrap.servers", getBootstrapServers());
        builder.put("mp.messaging.incoming.main-consumer.topic", topic);
        builder.put("mp.messaging.incoming.main-consumer.key.deserializer", StringDeserializer.class.getName());
        builder.put("mp.messaging.incoming.main-consumer.value.deserializer", StringDeserializer.class.getName());
        builder.put("mp.messaging.incoming.main-consumer.auto.offset.reset", "earliest");
        builder.put("mp.messaging.incoming.main-consumer.failure-strategy", "dead-letter-queue");
        builder.put("mp.messaging.incoming.main-consumer.dead-letter-queue.topic", topic + "-dlt");
        builder.put("mp.messaging.incoming.main-consumer.dead-letter-queue.key.serializer", StringSerializer.class.getName());
        builder.put("mp.messaging.incoming.main-consumer.dead-letter-queue.value.serializer", StringSerializer.class.getName());

        builder.put("mp.messaging.incoming.dlt-consumer.connector", CONNECTOR_NAME);
        builder.put("mp.messaging.incoming.dlt-consumer.bootstrap.servers", getBootstrapServers());
        builder.put("mp.messaging.incoming.dlt-consumer.topic", topic + "-dlt");
        builder.put("mp.messaging.incoming.dlt-consumer.key.deserializer", StringDeserializer.class.getName());
        builder.put("mp.messaging.incoming.dlt-consumer.value.deserializer", StringDeserializer.class.getName());
        builder.put("mp.messaging.incoming.dlt-consumer.auto.offset.reset", "earliest");

        return builder.build();
    }

    @ApplicationScoped
    public static class MyApp {
        private final List<KafkaRecord<String, String>> received = new CopyOnWriteArrayList<>();

        @Outgoing("main-producer")
        public Multi<KafkaRecord<String, String>> produce() {
            OutgoingKafkaRecord<String, String> record = KafkaRecord.of("my-key", "my-value")
                    .withHeader("my-header-key", "my-header-value");

            return Multi.createFrom().item(record);
        }

        @Incoming("main-consumer")
        public CompletionStage<Void> consume(KafkaRecord<String, String> msg) {
            return msg.nack(new Exception("Failed!"), Metadata.of(
                    OutgoingKafkaRecordMetadata.builder()
                            .withKey("my-failed-msg")
                            .withHeaders(new RecordHeaders()
                                    .add("my-failed-header-key", "my-failed-header-value".getBytes(StandardCharsets.UTF_8)))
                            .build()));
        }

        @Incoming("dlt-consumer")
        public CompletionStage<Void> consumeDeadLetterTopic(KafkaRecord<String, String> msg) {
            received.add(msg);
            return msg.ack();
        }

        public List<KafkaRecord<String, String>> received() {
            return received;
        }
    }
}
