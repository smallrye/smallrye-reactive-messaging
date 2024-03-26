package io.smallrye.reactive.messaging.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.regex.Pattern;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.GenericPayload;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.kafka.converters.ConsumerRecordConverter;
import io.smallrye.reactive.messaging.providers.helpers.GenericPayloadConverter;

public class GenericPayloadTest extends KafkaCompanionTestBase {
    private static final String TOPIC_NAME_BASE = "GenericPayloadTest-" + UUID.randomUUID() + "-";

    @Test
    public void testProduce() {
        for (int i = 0; i < 10; i++) {
            companion.topics().createAndWait(TOPIC_NAME_BASE + i, 1);
        }

        addBeans(GenericPayloadConverter.class);
        addBeans(ConsumerRecordConverter.class);
        runApplication(kafkaConfig("mp.messaging.outgoing.generated-producer")
                .put("topic", "nonexistent-topic")
                .put("key.serializer", StringSerializer.class.getName())
                .put("value.serializer", StringSerializer.class.getName()), MyApp.class);

        assertThat(companion.consumeStrings().fromTopics(Pattern.compile(TOPIC_NAME_BASE + ".+"), 10)
                .awaitCompletion()).allSatisfy(consumerRecord -> {
                    assertThat(consumerRecord.key()).startsWith("key-");
                    assertThat(consumerRecord.value()).startsWith("value-");
                    assertThat(consumerRecord.topic()).startsWith(TOPIC_NAME_BASE);

                    assertThat(consumerRecord.headers()).allSatisfy(header -> {
                        assertThat(header.key()).startsWith("my-header-");
                        assertThat(new String(header.value(), StandardCharsets.UTF_8)).startsWith("my-header-value-");
                    });
                });

    }

    @ApplicationScoped
    public static class MyApp {

        @Outgoing("generated-producer")
        public Multi<GenericPayload<String>> produce() {
            return Multi.createFrom().range(0, 10).map(id -> {
                Headers headersToBeUsed = new RecordHeaders()
                        .add("my-header-" + id, ("my-header-value-" + id).getBytes(StandardCharsets.UTF_8));
                return GenericPayload.of("value-" + id, Metadata.of(
                        OutgoingKafkaRecordMetadata.<String> builder()
                                .withTopic(TOPIC_NAME_BASE + id)
                                .withKey("key-" + id)
                                .withHeaders(headersToBeUsed)
                                .build()));
            });
        }

    }
}
