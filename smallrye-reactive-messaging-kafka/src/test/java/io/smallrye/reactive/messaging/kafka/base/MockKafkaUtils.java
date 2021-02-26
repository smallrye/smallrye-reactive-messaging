package io.smallrye.reactive.messaging.kafka.base;

import java.lang.reflect.Field;

import org.apache.kafka.clients.consumer.MockConsumer;

import io.smallrye.reactive.messaging.kafka.impl.KafkaSource;
import io.vertx.kafka.client.consumer.KafkaReadStream;
import io.vertx.mutiny.kafka.client.consumer.KafkaConsumer;

public class MockKafkaUtils {

    private MockKafkaUtils() {
        // Avoid direct instantiation
    }

    @SuppressWarnings("rawtypes")
    public static void injectMockConsumer(KafkaSource<String, String> source, MockConsumer<?, ?> consumer) {
        try {
            KafkaConsumer<String, String> cons = source.getConsumer();
            KafkaReadStream stream = cons.getDelegate().asStream();
            Field field = stream.getClass().getDeclaredField("consumer");
            field.setAccessible(true);
            field.set(stream, consumer);
            // Close the initial consumer.
            cons.closeAndAwait();
        } catch (Exception e) {
            throw new IllegalStateException("Unable to inject mock consumer", e);
        }
    }
}
