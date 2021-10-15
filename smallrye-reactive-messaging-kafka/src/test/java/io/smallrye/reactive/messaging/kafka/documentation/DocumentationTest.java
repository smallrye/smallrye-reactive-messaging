package io.smallrye.reactive.messaging.kafka.documentation;

import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;
import io.smallrye.reactive.messaging.kafka.base.KafkaTestBase;

/**
 * Checks the the snippet providing in the documentation are actually working.
 * Changing this tests requires changing the documentation.
 */
public class DocumentationTest extends KafkaTestBase {

    @Test
    public void testKafkaPriceConsumer() {
        KafkaPriceConsumer consumer = runApplication(getConsumerConfiguration(), KafkaPriceConsumer.class);

        await().until(this::isReady);

        AtomicInteger count = new AtomicInteger();
        usage.produceDoubles(50, null, () -> new ProducerRecord<>("prices", count.incrementAndGet() * 1.0));

        await().until(() -> consumer.list().size() >= 50);
    }

    @Test
    public void testMessageKafkaPriceConsumer() {
        KafkaPriceMessageConsumer consumer = runApplication(getConsumerConfiguration(), KafkaPriceMessageConsumer.class);

        await().until(this::isReady);

        AtomicInteger count = new AtomicInteger();
        usage.produceDoubles(50, null, () -> new ProducerRecord<>("prices", count.incrementAndGet() * 1.0));

        await().until(() -> consumer.list().size() >= 50);
    }

    @Test
    public void testKafkaPriceProducer() {
        List<Double> list = new CopyOnWriteArrayList<>();
        usage.consumeDoubles("prices", 50, 60, TimeUnit.SECONDS, null, (s, v) -> list.add(v));

        KafkaMapBasedConfig config = getProducerConfiguration();
        addConfig(config);
        weld.addBeanClass(KafkaPriceProducer.class);
        weld.disableDiscovery();
        container = weld.initialize();

        await().until(() -> list.size() >= 50);
    }

    @Test
    public void testKafkaPriceMessageProducer() {
        List<Double> list = new CopyOnWriteArrayList<>();
        usage.consumeDoubles("prices", 50, 60, TimeUnit.SECONDS, null, (s, v) -> list.add(v));

        KafkaMapBasedConfig config = getProducerConfiguration();
        addConfig(config);
        weld.addBeanClass(KafkaPriceMessageProducer.class);
        weld.disableDiscovery();
        container = weld.initialize();

        await().until(() -> list.size() >= 50);
    }

    private KafkaMapBasedConfig getConsumerConfiguration() {
        KafkaMapBasedConfig.Builder builder = KafkaMapBasedConfig.builder("mp.messaging.incoming.prices");
        builder.put("connector", "smallrye-kafka");
        builder.put("value.deserializer", "org.apache.kafka.common.serialization.DoubleDeserializer");
        return builder.build();
    }

    private KafkaMapBasedConfig getProducerConfiguration() {
        KafkaMapBasedConfig.Builder builder = KafkaMapBasedConfig.builder("mp.messaging.outgoing.prices");
        builder.put("connector", "smallrye-kafka");
        builder.put("value.serializer", "org.apache.kafka.common.serialization.DoubleSerializer");
        return builder.build();
    }

}
