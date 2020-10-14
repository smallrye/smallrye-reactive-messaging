package io.smallrye.reactive.messaging.kafka;

import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

import io.smallrye.reactive.messaging.kafka.base.KafkaTestBase;
import io.smallrye.reactive.messaging.kafka.base.MapBasedConfig;

public class EnvConfigTest extends KafkaTestBase {

    public static final String TOPIC_IN = "EnvConfigTest-IN";

    @Test
    @SetEnvironmentVariable(key = "MP_MESSAGING_INCOMING_KAFKA_TOPIC", value = TOPIC_IN)
    @SetEnvironmentVariable(key = "MP_MESSAGING_INCOMING_KAFKA_VALUE_DESERIALIZER", value = "org.apache.kafka.common.serialization.StringDeserializer")
    public void testConsumer() {
        KafkaConsumer bean = runApplication(MapBasedConfig.builder("mp.messaging.incoming.kafka")
                .put(
                        "auto.offset.reset", "earliest")
                .build(), KafkaConsumer.class);

        await().until(this::isReady);
        await().until(this::isAlive);
    }

    @ApplicationScoped
    public static class KafkaConsumer {

        private final List<Message<String>> messages = new CopyOnWriteArrayList<>();

        @Incoming("kafka")
        public CompletionStage<Void> consume(Message<String> incoming) {
            messages.add(incoming);
            return incoming.ack();
        }

        public List<Message<String>> getMessages() {
            return messages;
        }
    }
}
