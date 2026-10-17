package io.smallrye.reactive.messaging.kafka.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

class ReactiveKafkaProducerCloseTest {

    private ReactiveKafkaProducer<String, String> createProducer(int closeTimeout,
            AtomicReference<Producer<?, ?>> created) {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1");
        config.put(ProducerConfig.CLIENT_ID_CONFIG, "close-test");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new ReactiveKafkaProducer<>(config, "test", closeTimeout, false, null, null, null,
                t -> {
                }, (p, c) -> created.set(p));
    }

    @Test
    void closeWithZeroTimeoutClosesProducer() {
        ReactiveKafkaProducer<String, String> producer = createProducer(0, new AtomicReference<>());
        assertThat(producer.isClosed()).isFalse();

        assertThatCode(producer::close).doesNotThrowAnyException();
        assertThat(producer.isClosed()).isTrue();
    }

    @Test
    void closeWithPositiveTimeoutClosesProducer() {
        ReactiveKafkaProducer<String, String> producer = createProducer(1000, new AtomicReference<>());

        assertThatCode(producer::close).doesNotThrowAnyException();
        assertThat(producer.isClosed()).isTrue();
    }
}
