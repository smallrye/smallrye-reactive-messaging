package io.smallrye.reactive.messaging.kafka.documentation;

import static org.awaitility.Awaitility.await;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.After;
import org.junit.Test;

import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.reactive.messaging.kafka.KafkaTestBase;
import io.smallrye.reactive.messaging.kafka.KafkaUsage;
import io.smallrye.reactive.messaging.kafka.MapBasedConfig;

/**
 * Checks the the snippet providing in the documentation are actually working.
 * Changing this tests requires changing the documentation.
 */
public class DocumentationTest extends KafkaTestBase {

    private WeldContainer container;

    @After
    public void closing() {
        container.shutdown();
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
    }

    @Test
    public void testKafkaPriceConsumer() throws InterruptedException {
        MapBasedConfig config = getConsumerConfiguration();
        Weld weld = baseWeld();
        addConfig(config);
        weld.addBeanClass(KafkaPriceConsumer.class);
        weld.disableDiscovery();
        container = weld.initialize();

        KafkaPriceConsumer consumer = container.select(KafkaPriceConsumer.class).get();

        // TODO we need some readiness support in the connector
        Thread.sleep(1000);

        KafkaUsage usage = new KafkaUsage();
        AtomicInteger count = new AtomicInteger();
        usage.produceDoubles(50, null, () -> new ProducerRecord<>("prices", count.incrementAndGet() * 1.0));

        await().until(() -> consumer.list().size() >= 50);
    }

    @Test
    public void testMessageKafkaPriceConsumer() throws InterruptedException {
        MapBasedConfig config = getConsumerConfiguration();
        Weld weld = baseWeld();
        addConfig(config);
        weld.addBeanClass(KafkaPriceMessageConsumer.class);
        weld.disableDiscovery();
        container = weld.initialize();

        KafkaPriceMessageConsumer consumer = container.select(KafkaPriceMessageConsumer.class).get();

        // TODO we need some readiness support in the connector
        Thread.sleep(1000);

        KafkaUsage usage = new KafkaUsage();
        AtomicInteger count = new AtomicInteger();
        usage.produceDoubles(50, null, () -> new ProducerRecord<>("prices", count.incrementAndGet() * 1.0));

        await().until(() -> consumer.list().size() >= 50);
    }

    @Test
    public void testKafkaPriceProducer() {
        KafkaUsage usage = new KafkaUsage();
        List<Double> list = new CopyOnWriteArrayList<>();
        usage.consumeDoubles("prices", 50, 60, TimeUnit.SECONDS, null, (s, v) -> list.add(v));

        MapBasedConfig config = getProducerConfiguration();
        Weld weld = baseWeld();
        addConfig(config);
        weld.addBeanClass(KafkaPriceProducer.class);
        weld.disableDiscovery();
        container = weld.initialize();

        await().until(() -> list.size() >= 50);
    }

    @Test
    public void testKafkaPriceMessageProducer() {
        KafkaUsage usage = new KafkaUsage();
        List<Double> list = new CopyOnWriteArrayList<>();
        usage.consumeDoubles("prices", 50, 60, TimeUnit.SECONDS, null, (s, v) -> list.add(v));

        MapBasedConfig config = getProducerConfiguration();
        Weld weld = baseWeld();
        addConfig(config);
        weld.addBeanClass(KafkaPriceMessageProducer.class);
        weld.disableDiscovery();
        container = weld.initialize();

        await().until(() -> list.size() >= 50);
    }

    private MapBasedConfig getConsumerConfiguration() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("mp.messaging.incoming.prices.connector", "smallrye-kafka");
        conf.put("mp.messaging.incoming.prices.value.deserializer",
                "org.apache.kafka.common.serialization.DoubleDeserializer");
        return new MapBasedConfig(conf);
    }

    private MapBasedConfig getProducerConfiguration() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("mp.messaging.outgoing.prices.connector", "smallrye-kafka");
        conf.put("mp.messaging.outgoing.prices.value.serializer",
                "org.apache.kafka.common.serialization.DoubleSerializer");
        return new MapBasedConfig(conf);
    }

}
