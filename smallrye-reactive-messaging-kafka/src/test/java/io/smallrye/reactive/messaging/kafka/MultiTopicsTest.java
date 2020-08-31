package io.smallrye.reactive.messaging.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.spi.DeploymentException;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.smallrye.config.SmallRyeConfigProviderResolver;

/**
 * Test the Incoming connector when multiple topics are used either using a pattern or a list of topics.
 */
@SuppressWarnings("rawtypes")
public class MultiTopicsTest extends KafkaTestBase {

    private WeldContainer container;
    private KafkaUsage usage;

    @After
    public void cleanup() {
        if (container != null) {
            container.close();
        }
        // Release the config objects
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
    }

    @Before
    public void prepare() {
        usage = new KafkaUsage();
    }

    @Test
    public void testWithThreeTopicsInConfiguration() {
        String topic1 = UUID.randomUUID().toString();
        String topic2 = UUID.randomUUID().toString();
        String topic3 = UUID.randomUUID().toString();

        KafkaConsumer bean = deploy(new MapBasedConfig()
                .put("mp.messaging.incoming.kafka.connector", KafkaConnector.CONNECTOR_NAME)
                .put("mp.messaging.incoming.kafka.value.deserializer", StringDeserializer.class.getName())
                .put("mp.messaging.incoming.kafka.topics", topic1 + ", " + topic2 + ", " + topic3)
                .put("mp.messaging.incoming.kafka.tracing-enabled", false)
                .put("mp.messaging.incoming.kafka.auto.offset.reset", "earliest"));

        await().until(() -> isReady(container));
        await().until(() -> isLive(container));

        assertThat(bean.getMessages()).isEmpty();

        new Thread(() -> usage.produceStrings(3, null,
                () -> new ProducerRecord<>(topic1, "hello"))).start();

        new Thread(() -> usage.produceStrings(3, null,
                () -> new ProducerRecord<>(topic2, "hallo"))).start();

        new Thread(() -> usage.produceStrings(3, null,
                () -> new ProducerRecord<>(topic3, "bonjour"))).start();

        await().until(() -> bean.getMessages().size() >= 9);

        AtomicInteger top1 = new AtomicInteger();
        AtomicInteger top2 = new AtomicInteger();
        AtomicInteger top3 = new AtomicInteger();
        bean.getMessages().forEach(message -> {
            IncomingKafkaRecordMetadata record = message.getMetadata(IncomingKafkaRecordMetadata.class).orElse(null);
            assertThat(record).isNotNull();
            String topic = record.getTopic();
            if (topic.equals(topic1)) {
                top1.incrementAndGet();
                assertThat(message.getPayload()).isEqualTo("hello");
            } else if (topic.equals(topic2)) {
                top2.incrementAndGet();
                assertThat(message.getPayload()).isEqualTo("hallo");
            } else if (topic.equals(topic3)) {
                top3.incrementAndGet();
                assertThat(message.getPayload()).isEqualTo("bonjour");
            }
        });

        assertThat(top1).hasValue(3);
        assertThat(top2).hasValue(3);
        assertThat(top3).hasValue(3);
    }

    @Test
    public void testWithOnlyTwoTopicsReceiving() {
        String topic1 = UUID.randomUUID().toString();
        String topic2 = UUID.randomUUID().toString();
        String topic3 = UUID.randomUUID().toString();

        KafkaConsumer bean = deploy(new MapBasedConfig()
                .put("mp.messaging.incoming.kafka.connector", KafkaConnector.CONNECTOR_NAME)
                .put("mp.messaging.incoming.kafka.value.deserializer", StringDeserializer.class.getName())
                .put("mp.messaging.incoming.kafka.topics", topic1 + ", " + topic2 + ", " + topic3)
                .put("mp.messaging.incoming.kafka.tracing-enabled", false)
                .put("mp.messaging.incoming.kafka.auto.offset.reset", "earliest"));

        await().until(() -> isReady(container));
        await().until(() -> isLive(container));

        assertThat(bean.getMessages()).isEmpty();

        new Thread(() -> usage.produceStrings(3, null,
                () -> new ProducerRecord<>(topic1, "hello"))).start();

        new Thread(() -> usage.produceStrings(3, null,
                () -> new ProducerRecord<>(topic3, "bonjour"))).start();

        await().until(() -> bean.getMessages().size() >= 6);

        AtomicInteger top1 = new AtomicInteger();
        AtomicInteger top2 = new AtomicInteger();
        AtomicInteger top3 = new AtomicInteger();
        bean.getMessages().forEach(message -> {
            IncomingKafkaRecordMetadata record = message.getMetadata(IncomingKafkaRecordMetadata.class).orElse(null);
            assertThat(record).isNotNull();
            String topic = record.getTopic();
            if (topic.equals(topic1)) {
                top1.incrementAndGet();
                assertThat(message.getPayload()).isEqualTo("hello");
            } else if (topic.equals(topic2)) {
                top2.incrementAndGet();
            } else if (topic.equals(topic3)) {
                top3.incrementAndGet();
                assertThat(message.getPayload()).isEqualTo("bonjour");
            }
        });

        assertThat(top1).hasValue(3);
        assertThat(top2).hasValue(0);
        assertThat(top3).hasValue(3);
    }

    @Test
    public void testWithPattern() {
        String topic1 = "greetings-" + UUID.randomUUID().toString();
        String topic2 = "greetings-" + UUID.randomUUID().toString();
        String topic3 = "greetings-" + UUID.randomUUID().toString();

        kafka.createTopic(topic1, 1, 1);
        kafka.createTopic(topic2, 1, 1);
        kafka.createTopic(topic3, 1, 1);

        KafkaConsumer bean = deploy(new MapBasedConfig()
                .put("mp.messaging.incoming.kafka.connector", KafkaConnector.CONNECTOR_NAME)
                .put("mp.messaging.incoming.kafka.value.deserializer", StringDeserializer.class.getName())
                .put("mp.messaging.incoming.kafka.topic", "greetings-.+")
                .put("mp.messaging.incoming.kafka.pattern", true)
                .put("mp.messaging.incoming.kafka.tracing-enabled", false)
                .put("mp.messaging.incoming.kafka.auto.offset.reset", "earliest"));

        await().until(() -> isReady(container));
        await().until(() -> isLive(container));

        assertThat(bean.getMessages()).isEmpty();

        new Thread(() -> usage.produceStrings(3, null,
                () -> new ProducerRecord<>(topic1, "hello"))).start();

        new Thread(() -> usage.produceStrings(3, null,
                () -> new ProducerRecord<>(topic2, "hallo"))).start();

        new Thread(() -> usage.produceStrings(3, null,
                () -> new ProducerRecord<>(topic3, "bonjour"))).start();

        new Thread(() -> usage.produceStrings(3, null,
                () -> new ProducerRecord<>("do-not-match", "Bahh!"))).start();

        await().until(() -> bean.getMessages().size() >= 9);

        AtomicInteger top1 = new AtomicInteger();
        AtomicInteger top2 = new AtomicInteger();
        AtomicInteger top3 = new AtomicInteger();
        bean.getMessages().forEach(message -> {
            IncomingKafkaRecordMetadata record = message.getMetadata(IncomingKafkaRecordMetadata.class).orElse(null);
            assertThat(record).isNotNull();
            String topic = record.getTopic();
            if (topic.equals(topic1)) {
                top1.incrementAndGet();
                assertThat(message.getPayload()).isEqualTo("hello");
            } else if (topic.equals(topic2)) {
                top2.incrementAndGet();
                assertThat(message.getPayload()).isEqualTo("hallo");
            } else if (topic.equals(topic3)) {
                top3.incrementAndGet();
                assertThat(message.getPayload()).isEqualTo("bonjour");
            }
        });

        assertThat(top1).hasValue(3);
        assertThat(top2).hasValue(3);
        assertThat(top3).hasValue(3);
    }

    @Test
    public void testNonReadinessWithPatternIfTopicsAreNotCreated() {
        deploy(new MapBasedConfig()
                .put("mp.messaging.incoming.kafka.connector", KafkaConnector.CONNECTOR_NAME)
                .put("mp.messaging.incoming.kafka.value.deserializer", StringDeserializer.class.getName())
                .put("mp.messaging.incoming.kafka.topic", "greetings-.+")
                .put("mp.messaging.incoming.kafka.pattern", true)
                .put("mp.messaging.incoming.kafka.auto.offset.reset", "earliest"));

        await().until(() -> isLive(container));
        await()
                .pollDelay(10, TimeUnit.MILLISECONDS)
                .until(() -> !isReady(container));

    }

    @Test
    public void testInvalidConfigurations() {
        // Pattern and no topic
        assertThatThrownBy(() -> deploy(new MapBasedConfig()
                .put("mp.messaging.incoming.kafka.connector", KafkaConnector.CONNECTOR_NAME)
                .put("mp.messaging.incoming.kafka.value.deserializer", StringDeserializer.class.getName())
                .put("mp.messaging.incoming.kafka.pattern", true)))
                        .isInstanceOf(DeploymentException.class)
                        .hasCauseInstanceOf(IllegalArgumentException.class);

        // topics and no topic
        assertThatThrownBy(() -> deploy(new MapBasedConfig()
                .put("mp.messaging.incoming.kafka.connector", KafkaConnector.CONNECTOR_NAME)
                .put("mp.messaging.incoming.kafka.value.deserializer", StringDeserializer.class.getName())
                .put("mp.messaging.incoming.kafka.topic", "my-topic")
                .put("mp.messaging.incoming.kafka.topics", "a, b, c")))
                        .isInstanceOf(DeploymentException.class)
                        .hasCauseInstanceOf(IllegalArgumentException.class);

        // topics and pattern
        assertThatThrownBy(() -> deploy(new MapBasedConfig()
                .put("mp.messaging.incoming.kafka.connector", KafkaConnector.CONNECTOR_NAME)
                .put("mp.messaging.incoming.kafka.value.deserializer", StringDeserializer.class.getName())
                .put("mp.messaging.incoming.kafka.pattern", true)
                .put("mp.messaging.incoming.kafka.topics", "a, b, c"))).isInstanceOf(DeploymentException.class)
                        .hasCauseInstanceOf(IllegalArgumentException.class);
    }

    private KafkaConsumer deploy(MapBasedConfig config) {
        Weld weld = baseWeld();
        addConfig(config);
        weld.addBeanClass(KafkaConsumer.class);
        weld.disableDiscovery();
        container = weld.initialize();
        return container.getBeanManager().createInstance().select(KafkaConsumer.class).get();
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
