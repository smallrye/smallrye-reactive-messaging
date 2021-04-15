package io.smallrye.reactive.messaging.kafka.base;

import org.apache.kafka.clients.consumer.MockConsumer;

import io.smallrye.reactive.messaging.kafka.impl.KafkaSource;
import io.smallrye.reactive.messaging.kafka.impl.ReactiveKafkaConsumer;

public class MockKafkaUtils {

    private MockKafkaUtils() {
        // Avoid direct instantiation
    }

    @SuppressWarnings("rawtypes")
    public static void injectMockConsumer(KafkaSource<String, String> source, MockConsumer<?, ?> consumer) {
        try {
            ReactiveKafkaConsumer<String, String> client = source.getConsumer();
            client.injectClient(consumer);
        } catch (Exception e) {
            throw new IllegalStateException("Unable to inject mock consumer", e);
        }
    }
}
