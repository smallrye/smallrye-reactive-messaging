package io.smallrye.reactive.messaging.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.After;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import io.reactivex.Flowable;
import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.kafka.commit.KafkaThrottledLatestProcessedCommit;
import io.smallrye.reactive.messaging.kafka.impl.KafkaSink;

public class KafkaSinkTest extends KafkaTestBase {

    private WeldContainer container;
    private KafkaSink sink;

    @After
    public void cleanup() {
        if (sink != null) {
            sink.closeQuietly();
        }
        if (container != null) {
            container.close();
        }
        // Release the config objects
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
        KafkaThrottledLatestProcessedCommit.clearCache();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSinkUsingInteger() throws InterruptedException {
        KafkaUsage usage = new KafkaUsage();
        String topic = UUID.randomUUID().toString();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger expected = new AtomicInteger(0);
        usage.consumeIntegers(topic, 10, 10, TimeUnit.SECONDS,
                latch::countDown,
                (k, v) -> expected.getAndIncrement());

        Map<String, Object> config = getConfig();
        config.put("topic", topic);
        config.put("value.serializer", IntegerSerializer.class.getName());
        config.put("value.deserializer", IntegerDeserializer.class.getName());
        config.put("partition", 0);
        config.put("bootstrap.servers", SERVERS);
        config.put("channel-name", "testSinkUsingInteger");
        KafkaConnectorOutgoingConfiguration oc = new KafkaConnectorOutgoingConfiguration(new MapBasedConfig(config));
        sink = new KafkaSink(vertx, oc);

        Subscriber<? extends Message<?>> subscriber = sink.getSink().build();
        Flowable.range(0, 10)
                .map(Message::of)
                .subscribe((Subscriber<? super Message<Integer>>) subscriber);

        assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
        assertThat(expected).hasValue(10);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSinkUsingIntegerAndChannelName() throws InterruptedException {
        KafkaUsage usage = new KafkaUsage();
        String topic = UUID.randomUUID().toString();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger expected = new AtomicInteger(0);
        usage.consumeIntegers(topic, 10, 10, TimeUnit.SECONDS,
                latch::countDown,
                (k, v) -> expected.getAndIncrement());

        Map<String, Object> config = getConfig();
        config.put("channel-name", topic);
        config.put("value.serializer", IntegerSerializer.class.getName());
        config.put("value.deserializer", IntegerDeserializer.class.getName());
        config.put("partition", 0);
        config.put("bootstrap.servers", SERVERS);
        KafkaConnectorOutgoingConfiguration oc = new KafkaConnectorOutgoingConfiguration(new MapBasedConfig(config));
        sink = new KafkaSink(vertx, oc);

        Subscriber<? extends Message<?>> subscriber = sink.getSink().build();
        Flowable.range(0, 10)
                .map(Message::of)
                .subscribe((Subscriber<? super Message<Integer>>) subscriber);

        assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
        assertThat(expected).hasValue(10);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSinkUsingString() throws InterruptedException {
        KafkaUsage usage = new KafkaUsage();
        String topic = UUID.randomUUID().toString();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger expected = new AtomicInteger(0);
        usage.consumeStrings(topic, 10, 10, TimeUnit.SECONDS,
                latch::countDown,
                (k, v) -> expected.getAndIncrement());

        Map<String, Object> config = getConfig();
        config.put("topic", topic);
        config.put("value.serializer", StringSerializer.class.getName());
        config.put("value.deserializer", StringDeserializer.class.getName());
        config.put("partition", 0);
        config.put("bootstrap.servers", SERVERS);
        config.put("channel-name", "testSinkUsingString");
        KafkaConnectorOutgoingConfiguration oc = new KafkaConnectorOutgoingConfiguration(new MapBasedConfig(config));
        sink = new KafkaSink(vertx, oc);

        Subscriber<? extends Message<?>> subscriber = sink.getSink().build();
        Flowable.range(0, 10)
                .map(i -> Integer.toString(i))
                .map(Message::of)
                .subscribe((Subscriber<? super Message<String>>) subscriber);

        assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
        assertThat(expected).hasValue(10);
    }

    private Map<String, Object> getConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("bootstrap.servers", "localhost:9092");
        config.put("key.serializer", StringSerializer.class.getName());
        config.put("acks", "1");
        config.put("tracing-enabled", false);
        return config;
    }

    private MapBasedConfig getKafkaSinkConfigForProducingBean() {
        MapBasedConfig.ConfigBuilder builder = new MapBasedConfig.ConfigBuilder("mp.messaging.outgoing.output");
        builder.put("connector", KafkaConnector.CONNECTOR_NAME);
        builder.put("value.serializer", IntegerSerializer.class.getName());

        return new MapBasedConfig(builder.build());
    }

    private MapBasedConfig getKafkaSinkConfigForMessageProducingBean(String t) {
        MapBasedConfig.ConfigBuilder builder = new MapBasedConfig.ConfigBuilder("mp.messaging.outgoing.output-2");
        builder.put("connector", KafkaConnector.CONNECTOR_NAME);
        builder.put("value.serializer", IntegerSerializer.class.getName());
        if (t != null) {
            builder.put("topic", t);
        }

        return new MapBasedConfig(builder.build());
    }

    private MapBasedConfig getKafkaSinkConfigForRecordProducingBean(String t) {
        MapBasedConfig.ConfigBuilder builder = new MapBasedConfig.ConfigBuilder("mp.messaging.outgoing.output-record");
        builder.put("connector", KafkaConnector.CONNECTOR_NAME);
        builder.put("key.serializer", IntegerSerializer.class.getName());
        builder.put("value.serializer", StringSerializer.class.getName());
        if (t != null) {
            builder.put("topic", t);
        }

        return new MapBasedConfig(builder.build());
    }

    private void deploy(MapBasedConfig config, Class<?> clazz) {
        if (config != null) {
            config.write();
        } else {
            MapBasedConfig.clear();
        }

        Weld weld = baseWeld();
        weld.addBeanClass(clazz);
        container = weld.initialize();
    }

    @Test
    public void testABeanProducingMessagesSentToKafka() throws InterruptedException {
        deploy(getKafkaSinkConfigForProducingBean(), ProducingBean.class);

        KafkaUsage usage = new KafkaUsage();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger expected = new AtomicInteger(0);
        usage.consumeIntegers("output", 10, 10, TimeUnit.SECONDS,
                latch::countDown,
                (k, v) -> expected.getAndIncrement());

        await().until(() -> getHealth(container).getReadiness().isOk());

        assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
        assertThat(expected).hasValue(10);

        HealthReport liveness = getHealth(container).getLiveness();
        HealthReport readiness = getHealth(container).getReadiness();

        assertThat(liveness.isOk()).isTrue();
        assertThat(readiness.isOk()).isTrue();
        assertThat(liveness.getChannels()).hasSize(1);
        assertThat(readiness.getChannels()).hasSize(1);
        assertThat(liveness.getChannels().get(0).getChannel()).isEqualTo("output");
        assertThat(readiness.getChannels().get(0).getChannel()).isEqualTo("output");
    }

    @Test
    public void testABeanProducingKafkaMessagesSentToKafka() throws InterruptedException {
        KafkaUsage usage = new KafkaUsage();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger expected = new AtomicInteger(0);
        List<String> keys = new ArrayList<>();
        List<String> headers = new ArrayList<>();
        String topic = UUID.randomUUID().toString();
        usage.consumeIntegers(topic, 10, 10, TimeUnit.SECONDS,
                latch::countDown,
                record -> {
                    keys.add(record.key());
                    String count = new String(record.headers().lastHeader("count").value());
                    headers.add(count);
                    expected.getAndIncrement();
                });

        deploy(getKafkaSinkConfigForMessageProducingBean(topic), ProducingKafkaMessageBean.class);

        await().until(() -> getHealth(container).getReadiness().isOk());
        await().until(() -> getHealth(container).getLiveness().isOk());

        assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
        assertThat(expected).hasValue(10);
        assertThat(keys).containsExactly("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
        assertThat(headers).containsExactly("1", "2", "3", "4", "5", "6", "7", "8", "9", "10");
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void testInvalidType() throws InterruptedException {
        KafkaUsage usage = new KafkaUsage();
        String topic = UUID.randomUUID().toString();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger expected = new AtomicInteger(0);
        usage.consumeIntegers(topic, 10, 10, TimeUnit.SECONDS,
                latch::countDown,
                (k, v) -> expected.getAndIncrement());

        Map<String, Object> config = getConfig();
        config.put("topic", topic);
        config.put("value.serializer", IntegerSerializer.class.getName());
        config.put("value.deserializer", IntegerDeserializer.class.getName());
        config.put("partition", 0);
        config.put("max-inflight-messages", 1);
        config.put("bootstrap.servers", SERVERS);
        config.put("channel-name", "my-channel");
        config.put("failure-strategy", "ignore");
        config.put("retries", 0L); // disable retry.
        KafkaConnectorOutgoingConfiguration oc = new KafkaConnectorOutgoingConfiguration(new MapBasedConfig(config));
        sink = new KafkaSink(vertx, oc);

        await().until(() -> {
            HealthReport.HealthReportBuilder builder = HealthReport.builder();
            sink.isReady(builder);
            return builder.build().isOk();
        });

        List<Object> acked = new CopyOnWriteArrayList<>();
        List<Object> nacked = new CopyOnWriteArrayList<>();
        Subscriber subscriber = sink.getSink().build();
        Flowable.range(0, 6)
                .map(i -> {
                    if (i == 3 || i == 5) {
                        return Integer.toString(i);
                    }
                    return i;
                })
                .map(i -> Message.of(i, () -> {
                    acked.add(i);
                    return CompletableFuture.completedFuture(null);
                }, t -> {
                    nacked.add(i);
                    return CompletableFuture.completedFuture(null);
                }))
                .subscribe(subscriber);

        assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
        assertThat(expected).hasValue(4); // 3 and 5 are ignored.

        await().until(() -> nacked.size() >= 2);
        assertThat(acked).containsExactly(0, 1, 2, 4);
        assertThat(nacked).contains("3", "5");
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void testInvalidTypeWithDefaultInflightMessages() {
        KafkaUsage usage = new KafkaUsage();
        String topic = UUID.randomUUID().toString();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger expected = new AtomicInteger(0);
        usage.consumeIntegers(topic, 10, 10, TimeUnit.SECONDS,
                latch::countDown,
                (k, v) -> expected.getAndIncrement());

        Map<String, Object> config = getConfig();
        config.put("topic", topic);
        config.put("value.serializer", IntegerSerializer.class.getName());
        config.put("value.deserializer", IntegerDeserializer.class.getName());
        config.put("partition", 0);
        config.put("retries", 0L);
        config.put("bootstrap.servers", SERVERS);
        config.put("channel-name", "testInvalidTypeWithDefaultInflightMessages");
        KafkaConnectorOutgoingConfiguration oc = new KafkaConnectorOutgoingConfiguration(new MapBasedConfig(config));
        sink = new KafkaSink(vertx, oc);

        Subscriber subscriber = sink.getSink().build();
        Flowable.range(0, 5)
                .map(i -> {
                    if (i == 3 || i == 5) {
                        return Integer.toString(i);
                    }
                    return i;
                })
                .map(Message::of)
                .subscribe(subscriber);

        await().until(() -> expected.get() >= 3);
        // Default inflight is 5
        // 1, 2, 3, 4, 5 are sent at the same time.
        // As 3 fails, the stream is stopped, but, 1, 2, and 4 are already sent and potentially 6
        assertThat(expected).hasValueGreaterThanOrEqualTo(3);
    }

    @Test
    public void testABeanProducingMessagesUsingHeadersSentToKafka() throws InterruptedException {
        KafkaUsage usage = new KafkaUsage();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger expected = new AtomicInteger(0);
        List<String> keys = new ArrayList<>();
        List<String> headers = new ArrayList<>();
        String topic = UUID.randomUUID().toString();
        usage.consumeIntegers(topic, 10, 10, TimeUnit.SECONDS,
                latch::countDown,
                record -> {
                    keys.add(record.key());
                    String count = new String(record.headers().lastHeader("count").value());
                    headers.add(count);
                    expected.getAndIncrement();
                });

        deploy(getKafkaSinkConfigForMessageProducingBean(topic), ProducingMessageWithHeaderBean.class);

        assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
        assertThat(expected).hasValue(10);
        assertThat(keys).containsExactly("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
        assertThat(headers).containsExactly("1", "2", "3", "4", "5", "6", "7", "8", "9", "10");
    }

    // TODO key and value null, overriden by metadata

    @Test
    public void testABeanProducingRecords() throws InterruptedException {
        KafkaUsage usage = new KafkaUsage();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger expected = new AtomicInteger(0);
        List<Integer> keys = new ArrayList<>();
        List<String> values = new ArrayList<>();
        String topic = UUID.randomUUID().toString();
        String clientId = UUID.randomUUID().toString();
        usage.consume(clientId, clientId, OffsetResetStrategy.EARLIEST,
                new IntegerDeserializer(), new StringDeserializer(),
                () -> expected.get() < 10, null, latch::countDown, Collections.singletonList(topic),
                record -> {
                    keys.add(record.key());
                    values.add(record.value());
                    expected.getAndIncrement();
                });

        deploy(getKafkaSinkConfigForRecordProducingBean(topic), BeanProducingKafkaRecord.class);

        await().until(() -> getHealth(container).getReadiness().isOk());
        await().until(() -> getHealth(container).getLiveness().isOk());

        assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
        assertThat(expected).hasValue(10);
        assertThat(keys).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        assertThat(values)
                .containsExactly("value-1", "value-2", "value-3", "value-4", "value-5", "value-6", "value-7", "value-8",
                        "value-9", "value-10");
    }

    @Test
    public void testABeanProducingRecordsWithNullKey() throws InterruptedException {
        KafkaUsage usage = new KafkaUsage();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger expected = new AtomicInteger(0);
        List<Integer> keys = new ArrayList<>();
        List<String> values = new ArrayList<>();
        String topic = UUID.randomUUID().toString();
        String clientId = UUID.randomUUID().toString();
        usage.consume(clientId, clientId, OffsetResetStrategy.EARLIEST,
                new IntegerDeserializer(), new StringDeserializer(),
                () -> expected.get() < 10, null, latch::countDown, Collections.singletonList(topic),
                record -> {
                    keys.add(record.key());
                    values.add(record.value());
                    expected.getAndIncrement();
                });

        deploy(getKafkaSinkConfigForRecordProducingBean(topic), BeanProducingKafkaRecordNoKey.class);

        await().until(() -> getHealth(container).getReadiness().isOk());
        await().until(() -> getHealth(container).getLiveness().isOk());

        assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
        assertThat(expected).hasValue(10);
        assertThat(keys).containsExactly(null, null, null, null, null, null, null, null, null, null);
        assertThat(values)
                .containsExactly("value-1", "value-2", "value-3", "value-4", "value-5", "value-6", "value-7", "value-8",
                        "value-9", "value-10");
    }

    @Test
    public void testABeanProducingRecordsNoValue() throws InterruptedException {
        KafkaUsage usage = new KafkaUsage();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger expected = new AtomicInteger(0);
        List<Integer> keys = new ArrayList<>();
        List<String> values = new ArrayList<>();
        String topic = UUID.randomUUID().toString();
        String clientId = UUID.randomUUID().toString();
        usage.consume(clientId, clientId, OffsetResetStrategy.EARLIEST,
                new IntegerDeserializer(), new StringDeserializer(),
                () -> expected.get() < 10, null, latch::countDown, Collections.singletonList(topic),
                record -> {
                    keys.add(record.key());
                    values.add(record.value());
                    expected.getAndIncrement();
                });

        deploy(getKafkaSinkConfigForRecordProducingBean(topic), BeanProducingKafkaRecordNoValue.class);

        await().until(() -> getHealth(container).getReadiness().isOk());
        await().until(() -> getHealth(container).getLiveness().isOk());

        assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
        assertThat(expected).hasValue(10);
        assertThat(keys).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        assertThat(values)
                .containsExactly(null, null, null, null, null, null, null, null, null, null);
    }

    @Test
    public void testABeanProducingRecordsNoValueNoKey() throws InterruptedException {
        KafkaUsage usage = new KafkaUsage();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger expected = new AtomicInteger(0);
        List<Integer> keys = new ArrayList<>();
        List<String> values = new ArrayList<>();
        String topic = UUID.randomUUID().toString();
        String clientId = UUID.randomUUID().toString();
        usage.consume(clientId, clientId, OffsetResetStrategy.EARLIEST,
                new IntegerDeserializer(), new StringDeserializer(),
                () -> expected.get() < 10, null, latch::countDown, Collections.singletonList(topic),
                record -> {
                    keys.add(record.key());
                    values.add(record.value());
                    expected.getAndIncrement();
                });

        deploy(getKafkaSinkConfigForRecordProducingBean(topic), BeanProducingKafkaRecordNoValueNoKey.class);

        await().until(() -> getHealth(container).getReadiness().isOk());
        await().until(() -> getHealth(container).getLiveness().isOk());

        assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
        assertThat(expected).hasValue(10);
        assertThat(keys).containsExactly(null, null, null, null, null, null, null, null, null, null);
        assertThat(values)
                .containsExactly(null, null, null, null, null, null, null, null, null, null);
    }

    @Test
    public void testABeanProducingRecordsAsMessageWithKeyOverridden() throws InterruptedException {
        KafkaUsage usage = new KafkaUsage();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger expected = new AtomicInteger(0);
        List<Integer> keys = new ArrayList<>();
        List<String> values = new ArrayList<>();
        String topic = UUID.randomUUID().toString();
        String clientId = UUID.randomUUID().toString();
        usage.consume(clientId, clientId, OffsetResetStrategy.EARLIEST,
                new IntegerDeserializer(), new StringDeserializer(),
                () -> expected.get() < 10, null, latch::countDown, Collections.singletonList(topic),
                record -> {
                    keys.add(record.key());
                    values.add(record.value());
                    expected.getAndIncrement();
                });

        deploy(getKafkaSinkConfigForRecordProducingBean(topic), BeanProducingKafkaRecordInMessage.class);

        await().until(() -> getHealth(container).getReadiness().isOk());
        await().until(() -> getHealth(container).getLiveness().isOk());

        assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
        assertThat(expected).hasValue(10);
        assertThat(keys).containsExactly(100, 1, 102, 3, 104, 5, 106, 7, 108, 9);
        assertThat(values)
                .containsExactly("value-1", "value-2", "value-3", "value-4", "value-5", "value-6", "value-7", "value-8",
                        "value-9", "value-10");
    }

    @ApplicationScoped
    public static class BeanProducingKafkaRecord {

        @Incoming("data")
        @Outgoing("output-record")
        public Record<Integer, String> process(int input) {
            return Record.of(input, "value-" + (input + 1));
        }

        @Outgoing("data")
        public Publisher<Integer> source() {
            return Flowable.range(0, 10);
        }

    }

    @ApplicationScoped
    public static class BeanProducingKafkaRecordNoKey {

        @Incoming("data")
        @Outgoing("output-record")
        public Record<Integer, String> process(int input) {
            return Record.of(null, "value-" + (input + 1));
        }

        @Outgoing("data")
        public Publisher<Integer> source() {
            return Flowable.range(0, 10);
        }

    }

    @ApplicationScoped
    public static class BeanProducingKafkaRecordNoValue {

        @Incoming("data")
        @Outgoing("output-record")
        public Record<Integer, String> process(int input) {
            return Record.of(input, null);
        }

        @Outgoing("data")
        public Publisher<Integer> source() {
            return Flowable.range(0, 10);
        }

    }

    @ApplicationScoped
    public static class BeanProducingKafkaRecordNoValueNoKey {

        @Incoming("data")
        @Outgoing("output-record")
        public Record<Integer, String> process(int input) {
            return Record.of(null, null);
        }

        @Outgoing("data")
        public Publisher<Integer> source() {
            return Flowable.range(0, 10);
        }

    }

    @ApplicationScoped
    public static class BeanProducingKafkaRecordInMessage {

        @Incoming("data")
        @Outgoing("output-record")
        public Message<Record<Integer, String>> process(Message<Integer> input) {
            int value = input.getPayload();
            if (value % 2 != 0) {
                return input.withPayload(Record.of(value, "value-" + (value + 1)));
            } else {
                OutgoingKafkaRecordMetadata<Integer> metadata = OutgoingKafkaRecordMetadata.<Integer> builder()
                        .withKey(100 + value).build();
                return input.withPayload(Record.of(value, "value-" + (value + 1))).addMetadata(metadata);
            }
        }

        @Outgoing("data")
        public Publisher<Integer> source() {
            return Flowable.range(0, 10);
        }

    }

}
