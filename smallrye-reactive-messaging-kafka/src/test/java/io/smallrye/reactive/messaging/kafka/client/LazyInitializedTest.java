package io.smallrye.reactive.messaging.kafka.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Flow;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.kafka.CountKafkaCdiEvents;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorOutgoingConfiguration;
import io.smallrye.reactive.messaging.kafka.KafkaProducer;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import io.smallrye.reactive.messaging.kafka.base.UnsatisfiedInstance;
import io.smallrye.reactive.messaging.kafka.base.WeldTestBase;
import io.smallrye.reactive.messaging.kafka.impl.KafkaSink;
import io.smallrye.reactive.messaging.kafka.impl.KafkaSource;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.mutiny.core.Vertx;

public class LazyInitializedTest extends WeldTestBase {

    @Test
    void testLazyInitializedProducer() {
        Map<String, Object> props = new HashMap<>();
        props.put("tracing-enabled", false);
        props.put("lazy-client", true);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "invalid-bootstrap-servers");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put("channel-name", "test-" + ThreadLocalRandom.current().nextInt());
        props.put("topic", "topic");
        MapBasedConfig config = new MapBasedConfig(props);

        KafkaSink sink = new KafkaSink(new KafkaConnectorOutgoingConfiguration(config),
                CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance(), UnsatisfiedInstance.instance());

        KafkaProducer<?, ?> producer = sink.getProducer();
        assertThat(producer).isNotNull();
        assertThat(producer.unwrap()).isNull();

        Flow.Subscriber<? extends Message<?>> subscriber = sink.getSink();
        await().untilAsserted(() -> {
            assertThat(getHealthReport(sink::isStarted).isOk()).isTrue();
            assertThat(getHealthReport(sink::isReady).isOk()).isTrue();
            assertThat(getHealthReport(sink::isAlive).isOk()).isTrue();
        });

        Multi.createFrom().<Message<?>> item(KafkaRecord.of(1, ""))
                .subscribe((Flow.Subscriber<? super Message<?>>) subscriber);
        await().untilAsserted(() -> {
            assertThat(getHealthReport(sink::isStarted).isOk()).isTrue();
            assertThat(getHealthReport(sink::isReady).isOk()).isTrue();
            assertThat(getHealthReport(sink::isAlive).isOk()).isFalse();
        });

        sink.closeQuietly();
    }

    @Test
    void testEagerInitializedProducer() {
        Map<String, Object> props = new HashMap<>();
        props.put("tracing-enabled", false);
        props.put("lazy-client", false);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "invalid-bootstrap-servers");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        String channelName = "test-" + ThreadLocalRandom.current().nextInt();
        props.put("channel-name", channelName);
        props.put("topic", "topic");
        MapBasedConfig config = new MapBasedConfig(props);

        assertThatThrownBy(() -> {
            KafkaSink sink = new KafkaSink(new KafkaConnectorOutgoingConfiguration(config),
                    CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance(), UnsatisfiedInstance.instance());
        }).hasCauseInstanceOf(KafkaException.class);

    }

    @Test
    void testLazyInitializedConsumer() {
        String groupId = UUID.randomUUID().toString();
        Map<String, Object> props = new HashMap<>();
        props.put("channel-name", "test");
        props.put("lazy-client", true);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "invalid-bootstrap-servers");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put("topic", "topic");
        new MapBasedConfig(props);
        MapBasedConfig config = new MapBasedConfig(props);

        KafkaSource<Object, Object> source = new KafkaSource<>(Vertx.vertx(), groupId,
                new KafkaConnectorIncomingConfiguration(config),
                commitHandlerFactories,
                failureHandlerFactories,
                UnsatisfiedInstance.instance(),
                CountKafkaCdiEvents.noCdiEvents,
                UnsatisfiedInstance.instance(),
                0);

        assertThat(source.getConsumer().unwrap()).isNull();
        await().untilAsserted(() -> {
            assertThat(getHealthReport(source::isStarted).isOk()).isTrue();
            assertThat(getHealthReport(source::isReady).isOk()).isTrue();
            assertThat(getHealthReport(source::isAlive).isOk()).isTrue();
        });

        source.getStream().subscribe(AssertSubscriber.create());
        await().untilAsserted(() -> {
            assertThat(getHealthReport(source::isStarted).isOk()).isTrue();
            assertThat(getHealthReport(source::isReady).isOk()).isTrue();
            assertThat(getHealthReport(source::isAlive).isOk()).isFalse();
        });

        source.closeQuietly();
    }

    @Test
    void testEagerInitializedConsumer() {
        String groupId = UUID.randomUUID().toString();
        Map<String, Object> props = new HashMap<>();
        props.put("channel-name", "test");
        props.put("lazy-client", false);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "invalid-bootstrap-servers");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put("topic", "topic");
        MapBasedConfig config = new MapBasedConfig(props);

        assertThatThrownBy(() -> {
            KafkaSource<Object, Object> source = new KafkaSource<>(Vertx.vertx(), groupId,
                    new KafkaConnectorIncomingConfiguration(config),
                    commitHandlerFactories,
                    failureHandlerFactories,
                    UnsatisfiedInstance.instance(),
                    CountKafkaCdiEvents.noCdiEvents,
                    UnsatisfiedInstance.instance(),
                    0);
        }).hasCauseInstanceOf(KafkaException.class);

    }

    HealthReport getHealthReport(Consumer<HealthReport.HealthReportBuilder> builderConsumer) {
        HealthReport.HealthReportBuilder builder = HealthReport.builder();
        builderConsumer.accept(builder);
        return builder.build();
    }
}
