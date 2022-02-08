package io.smallrye.reactive.messaging.kafka.documentation;

import static org.awaitility.Awaitility.await;

import java.time.Duration;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;
import io.smallrye.reactive.messaging.kafka.companion.ConsumerTask;

/**
 * Checks the the snippet providing in the documentation are actually working.
 * Changing this tests requires changing the documentation.
 */
public class DocumentationTest extends KafkaCompanionTestBase {

    @Test
    public void testKafkaPriceConsumer() {
        KafkaPriceConsumer consumer = runApplication(getConsumerConfiguration(), KafkaPriceConsumer.class);

        await().until(this::isReady);

        companion.produceDoubles().usingGenerator(i -> new ProducerRecord<>("prices", i * 1.0), 50);

        await().until(() -> consumer.list().size() >= 50);
    }

    @Test
    public void testMessageKafkaPriceConsumer() {
        KafkaPriceMessageConsumer consumer = runApplication(getConsumerConfiguration(), KafkaPriceMessageConsumer.class);

        await().until(this::isReady);

        companion.produceDoubles().usingGenerator(i -> new ProducerRecord<>("prices", i * 1.0), 50);

        await().until(() -> consumer.list().size() >= 50);
    }

    @Test
    public void testKafkaPriceProducer() {
        ConsumerTask<String, Double> prices = companion.consumeDoubles()
                .fromTopics("prices", 50, Duration.ofSeconds(60));

        KafkaMapBasedConfig config = getProducerConfiguration();
        addConfig(config);
        weld.addBeanClass(KafkaPriceProducer.class);
        weld.disableDiscovery();
        container = weld.initialize();

        await().until(() -> prices.getRecords().size() >= 50);
    }

    @Test
    public void testKafkaPriceMessageProducer() {
        ConsumerTask<String, Double> prices = companion.consumeDoubles()
                .fromTopics("prices", 50, Duration.ofSeconds(60));

        KafkaMapBasedConfig config = getProducerConfiguration();
        addConfig(config);
        weld.addBeanClass(KafkaPriceMessageProducer.class);
        weld.disableDiscovery();
        container = weld.initialize();

        await().until(() -> prices.getRecords().size() >= 50);
    }

    private KafkaMapBasedConfig getConsumerConfiguration() {
        KafkaMapBasedConfig config = kafkaConfig("mp.messaging.incoming.prices");
        config.put("connector", "smallrye-kafka");
        config.put("value.deserializer", "org.apache.kafka.common.serialization.DoubleDeserializer");
        return config;
    }

    private KafkaMapBasedConfig getProducerConfiguration() {
        KafkaMapBasedConfig config = kafkaConfig("mp.messaging.outgoing.prices");
        config.put("connector", "smallrye-kafka");
        config.put("value.serializer", "org.apache.kafka.common.serialization.DoubleSerializer");
        return config;
    }

}
