package io.smallrye.reactive.messaging.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;
import io.smallrye.reactive.messaging.kafka.base.UnsatisfiedInstance;
import io.smallrye.reactive.messaging.kafka.companion.ConsumerTask;
import io.smallrye.reactive.messaging.kafka.companion.KafkaCompanion;
import io.smallrye.reactive.messaging.kafka.impl.KafkaSink;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class KafkaSinkTest extends KafkaCompanionTestBase {

    private KafkaSink sink;

    @AfterEach
    public void cleanup() {
        if (sink != null) {
            sink.closeQuietly();
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSinkUsingInteger() {
        ConsumerTask<String, Integer> consumed = companion.consumeIntegers().fromTopics(topic, 10, Duration.ofSeconds(10));

        MapBasedConfig config = getBaseConfig()
                .with("topic", topic)
                .with("value.serializer", IntegerSerializer.class.getName())
                .with("partition", 0)
                .with("channel-name", "testSinkUsingInteger");
        KafkaConnectorOutgoingConfiguration oc = new KafkaConnectorOutgoingConfiguration(config);
        sink = new KafkaSink(oc, CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance());

        Subscriber<? extends Message<?>> subscriber = sink.getSink().build();
        Multi.createFrom().range(0, 10)
                .map(Message::of)
                .subscribe((Subscriber<? super Message<Integer>>) subscriber);

        assertThat(consumed.awaitCompletion(Duration.ofMinutes(1)).count()).isEqualTo(10);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSinkUsingIntegerAndChannelName() {
        ConsumerTask<String, Integer> consumed = companion.consumeIntegers().fromTopics(topic, 10, Duration.ofSeconds(10));

        MapBasedConfig config = getBaseConfig()
                .with("channel-name", topic)
                .with("value.serializer", IntegerSerializer.class.getName())
                .with("partition", 0);
        KafkaConnectorOutgoingConfiguration oc = new KafkaConnectorOutgoingConfiguration(config);
        sink = new KafkaSink(oc, CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance());

        Subscriber<? extends Message<?>> subscriber = sink.getSink().build();
        Multi.createFrom().range(0, 10)
                .map(Message::of)
                .subscribe((Subscriber<? super Message<Integer>>) subscriber);

        assertThat(consumed.awaitCompletion(Duration.ofMinutes(1)).count()).isEqualTo(10);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSinkUsingString() {
        ConsumerTask<String, String> consumed = companion.consumeStrings().fromTopics(topic, 10, Duration.ofSeconds(10));

        MapBasedConfig config = getBaseConfig()
                .with("topic", topic)
                .with("value.serializer", StringSerializer.class.getName())
                .with("partition", 0)
                .with("channel-name", "testSinkUsingString");
        KafkaConnectorOutgoingConfiguration oc = new KafkaConnectorOutgoingConfiguration(config);
        sink = new KafkaSink(oc, CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance());

        Subscriber<? extends Message<?>> subscriber = sink.getSink().build();
        Multi.createFrom().range(0, 10)
                .map(i -> Integer.toString(i))
                .map(Message::of)
                .subscribe((Subscriber<? super Message<String>>) subscriber);

        assertThat(consumed.awaitCompletion(Duration.ofMinutes(1)).count()).isEqualTo(10);
    }

    private MapBasedConfig getBaseConfig() {
        return kafkaConfig()
                .put("key.serializer", StringSerializer.class.getName())
                .put("acks", "1");
    }

    private MapBasedConfig getKafkaSinkConfigForProducingBean() {
        return kafkaConfig("mp.messaging.outgoing.output")
                .put("value.serializer", IntegerSerializer.class.getName());
    }

    private MapBasedConfig getKafkaSinkConfigForMessageProducingBean() {
        return kafkaConfig("mp.messaging.outgoing.output-2")
                .put("value.serializer", IntegerSerializer.class.getName())
                .put("topic", topic);
    }

    private KafkaMapBasedConfig getKafkaSinkConfigForRecordProducingBean(String t) {
        KafkaMapBasedConfig config = kafkaConfig("mp.messaging.outgoing.output-record");
        config.put("key.serializer", IntegerSerializer.class.getName());
        config.put("value.serializer", StringSerializer.class.getName());
        if (t != null) {
            config.put("topic", t);
        }

        return config;
    }

    private KafkaMapBasedConfig getKafkaSinkConfigWithMultipleUpstreams(String t) {
        KafkaMapBasedConfig config = kafkaConfig("mp.messaging.outgoing.data");
        config.put("key.serializer", StringSerializer.class.getName());
        config.put("value.serializer", IntegerSerializer.class.getName());
        config.put("merge", true);
        if (t != null) {
            config.put("topic", t);
        }

        return config;
    }

    @Test
    public void testABeanProducingMessagesSentToKafka() {
        runApplication(getKafkaSinkConfigForProducingBean(), ProducingBean.class);

        ConsumerTask<String, Integer> consumed = companion.consumeIntegers().fromTopics("output", 10,
                Duration.ofSeconds(10));

        await().until(this::isReady);

        assertThat(consumed.awaitCompletion(Duration.ofMinutes(1)).count()).isEqualTo(10);

        HealthReport liveness = getHealth().getLiveness();
        HealthReport readiness = getHealth().getReadiness();

        assertThat(liveness.isOk()).isTrue();
        assertThat(readiness.isOk()).isTrue();
        assertThat(liveness.getChannels()).hasSize(1);
        assertThat(readiness.getChannels()).hasSize(1);
        assertThat(liveness.getChannels().get(0).getChannel()).isEqualTo("output");
        assertThat(readiness.getChannels().get(0).getChannel()).isEqualTo("output");

        KafkaClientService service = get(KafkaClientService.class);
        assertThat(service.getProducer("output")).isNotNull();
        assertThat(service.getProducer("missing")).isNull();
        assertThatThrownBy(() -> service.getProducer(null)).isInstanceOf(NullPointerException.class);
        assertThat(service.getProducerChannels()).containsExactly("output");
    }

    @Test
    public void testABeanProducingKafkaMessagesSentToKafka() {
        ConsumerTask<String, Integer> consumed = companion.consumeIntegers().fromTopics(topic, 10, Duration.ofSeconds(10));

        runApplication(getKafkaSinkConfigForMessageProducingBean(), ProducingKafkaMessageBean.class);

        await().until(this::isReady);
        await().until(this::isAlive);

        assertThat(consumed.awaitCompletion(Duration.ofMinutes(1)).count()).isEqualTo(10);
        assertThat(consumed.getRecords())
                .extracting(ConsumerRecord::key)
                .containsExactly("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
        assertThat(consumed.getRecords())
                .extracting(cr -> KafkaCompanion.getHeader(cr.headers(), "count"))
                .containsExactly("1", "2", "3", "4", "5", "6", "7", "8", "9", "10");
    }

    @Test
    public void testABeanProducingKafkaMessagesSentToKafkaUsingAdminHealthCheck() {
        ConsumerTask<String, Integer> consumed = companion.consumeIntegers().fromTopics(topic, 10, Duration.ofSeconds(10));

        runApplication(getKafkaSinkConfigForMessageProducingBean()
                .with("health-readiness-topic-verification", true),
                ProducingKafkaMessageBean.class);

        await().until(this::isReady);
        await().until(this::isAlive);

        assertThat(consumed.awaitCompletion(Duration.ofMinutes(1)).count()).isEqualTo(10);
        assertThat(consumed.getRecords())
                .extracting(ConsumerRecord::key)
                .containsExactly("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
        assertThat(consumed.getRecords())
                .extracting(cr -> KafkaCompanion.getHeader(cr.headers(), "count"))
                .containsExactly("1", "2", "3", "4", "5", "6", "7", "8", "9", "10");
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void testInvalidPayloadType() {
        ConsumerTask<String, Integer> consumed = companion.consumeIntegers().fromTopics(topic, 4, Duration.ofSeconds(10));

        MapBasedConfig config = getBaseConfig()
                .with("topic", topic)
                .with("value.serializer", IntegerSerializer.class.getName())
                .with("partition", 0)
                .with("max-inflight-messages", 1L)
                .with("channel-name", "my-channel")
                .with("retries", 0L); // disable retry.
        KafkaConnectorOutgoingConfiguration oc = new KafkaConnectorOutgoingConfiguration(config);
        CountKafkaCdiEvents testCdiEvents = new CountKafkaCdiEvents();
        sink = new KafkaSink(oc, testCdiEvents, UnsatisfiedInstance.instance());

        await().until(() -> {
            HealthReport.HealthReportBuilder builder = HealthReport.builder();
            sink.isReady(builder);
            return builder.build().isOk();
        });

        List<Object> acked = new CopyOnWriteArrayList<>();
        List<Object> nacked = new CopyOnWriteArrayList<>();
        Subscriber subscriber = sink.getSink().build();
        Multi.createFrom().range(0, 6)
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

        assertThat(consumed.awaitCompletion(Duration.ofMinutes(1)).count()).isEqualTo(4);

        await().until(() -> nacked.size() >= 2);
        assertThat(acked).containsExactly(0, 1, 2, 4);
        assertThat(nacked).contains("3", "5");

        assertThat(testCdiEvents.firedConsumerEvents.sum()).isEqualTo(0);
        assertThat(testCdiEvents.firedProducerEvents.sum()).isEqualTo(1);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void testInvalidTypeWithDefaultInflightMessages() {
        ConsumerTask<String, Integer> consumed = companion.consumeIntegers().fromTopics(topic, 10, Duration.ofSeconds(10));

        MapBasedConfig config = getBaseConfig()
                .with("topic", topic)
                .with("value.serializer", IntegerSerializer.class.getName())
                .with("partition", 0)
                .with("retries", 0L)
                .with("channel-name", "testInvalidTypeWithDefaultInflightMessages");
        KafkaConnectorOutgoingConfiguration oc = new KafkaConnectorOutgoingConfiguration(config);
        sink = new KafkaSink(oc, CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance());

        Subscriber subscriber = sink.getSink().build();
        Multi.createFrom().range(0, 5)
                .map(i -> {
                    if (i == 3 || i == 5) {
                        return Integer.toString(i);
                    }
                    return i;
                })
                .map(Message::of)
                .subscribe(subscriber);

        await().until(() -> consumed.count() >= 3);
        // Default inflight is 5
        // 1, 2, 3, 4, 5 are sent at the same time.
        // As 3 fails, the stream is stopped, but, 1, 2, and 4 are already sent and potentially 6
        assertThat(consumed.count()).isGreaterThanOrEqualTo(3);
    }

    @Test
    public void testABeanProducingMessagesUsingHeadersSentToKafka() {
        ConsumerTask<String, Integer> consumed = companion.consumeIntegers().fromTopics(topic, 10, Duration.ofSeconds(10));

        runApplication(getKafkaSinkConfigForMessageProducingBean(), ProducingMessageWithHeaderBean.class);

        assertThat(consumed.awaitCompletion(Duration.ofMinutes(1)).count()).isEqualTo(10);
        assertThat(consumed.getRecords())
                .extracting(ConsumerRecord::key)
                .containsExactly("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
        assertThat(consumed.getRecords())
                .extracting(cr -> KafkaCompanion.getHeader(cr.headers(), "count"))
                .containsExactly("1", "2", "3", "4", "5", "6", "7", "8", "9", "10");
    }

    @Test
    public void testABeanProducingRecords() {
        ConsumerTask<Integer, String> consumed = companion.consume(Integer.class, String.class)
                .fromTopics(topic, 10);

        runApplication(getKafkaSinkConfigForRecordProducingBean(topic), BeanProducingKafkaRecord.class);

        await().until(this::isReady);
        await().until(this::isAlive);

        assertThat(consumed.awaitCompletion(Duration.ofMinutes(1)).count()).isEqualTo(10);
        assertThat(consumed.getRecords())
                .extracting(ConsumerRecord::key)
                .containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        assertThat(consumed.getRecords())
                .extracting(ConsumerRecord::value)
                .containsExactly("value-1", "value-2", "value-3", "value-4", "value-5", "value-6", "value-7", "value-8",
                        "value-9", "value-10");
    }

    @Test
    public void testABeanProducingRecordsWithNullKey() {
        ConsumerTask<Integer, String> consumed = companion.consume(Integer.class, String.class)
                .fromTopics(topic, 10);

        runApplication(getKafkaSinkConfigForRecordProducingBean(topic), BeanProducingKafkaRecordNoKey.class);

        await().until(this::isReady);
        await().until(this::isAlive);

        assertThat(consumed.awaitCompletion(Duration.ofMinutes(1)).count()).isEqualTo(10);
        assertThat(consumed.getRecords())
                .extracting(ConsumerRecord::key)
                .containsExactly(null, null, null, null, null, null, null, null, null, null);
        assertThat(consumed.getRecords())
                .extracting(ConsumerRecord::value)
                .containsExactly("value-1", "value-2", "value-3", "value-4", "value-5", "value-6", "value-7", "value-8",
                        "value-9", "value-10");
    }

    @Test
    public void testABeanProducingRecordsNoValue() {
        ConsumerTask<Integer, String> consumed = companion.consume(Integer.class, String.class)
                .fromTopics(topic, 10);

        runApplication(getKafkaSinkConfigForRecordProducingBean(topic), BeanProducingKafkaRecordNoValue.class);

        await().until(this::isReady);
        await().until(this::isAlive);

        assertThat(consumed.awaitCompletion(Duration.ofMinutes(1)).count()).isEqualTo(10);
        assertThat(consumed.getRecords())
                .extracting(ConsumerRecord::key)
                .containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        assertThat(consumed.getRecords())
                .extracting(ConsumerRecord::value)
                .containsExactly(null, null, null, null, null, null, null, null, null, null);
    }

    @Test
    public void testABeanProducingRecordsNoValueNoKey() throws InterruptedException {
        ConsumerTask<Integer, String> consumed = companion.consume(Integer.class, String.class)
                .fromTopics(topic, 10);

        runApplication(getKafkaSinkConfigForRecordProducingBean(topic), BeanProducingKafkaRecordNoValueNoKey.class);

        await().until(this::isReady);
        await().until(this::isAlive);

        assertThat(consumed.awaitCompletion(Duration.ofMinutes(1)).count()).isEqualTo(10);
        assertThat(consumed.getRecords())
                .extracting(ConsumerRecord::key)
                .containsExactly(null, null, null, null, null, null, null, null, null, null);
        assertThat(consumed.getRecords())
                .extracting(ConsumerRecord::value)
                .containsExactly(null, null, null, null, null, null, null, null, null, null);
    }

    @Test
    public void testABeanProducingRecordsAsMessageWithKeyOverridden() {
        ConsumerTask<Integer, String> consumed = companion.consume(Integer.class, String.class)
                .fromTopics(topic, 10);

        runApplication(getKafkaSinkConfigForRecordProducingBean(topic), BeanProducingKafkaRecordInMessage.class);

        await().until(this::isReady);
        await().until(this::isAlive);

        assertThat(consumed.awaitCompletion(Duration.ofMinutes(1)).count()).isEqualTo(10);
        assertThat(consumed.getRecords())
                .extracting(ConsumerRecord::key)
                .containsExactly(100, 1, 102, 3, 104, 5, 106, 7, 108, 9);
        assertThat(consumed.getRecords())
                .extracting(ConsumerRecord::value)
                .containsExactly("value-1", "value-2", "value-3", "value-4", "value-5", "value-6", "value-7", "value-8",
                        "value-9", "value-10");
    }

    @Test
    public void testConnectorWithMultipleUpstreams() {
        ConsumerTask<String, Integer> consumed = companion.consumeIntegers().fromTopics(topic, 20);

        KafkaMapBasedConfig config = getKafkaSinkConfigWithMultipleUpstreams(topic);
        runApplication(config, BeanWithMultipleUpstreams.class);

        await().until(this::isReady);
        await().until(this::isAlive);

        assertThat(consumed.awaitCompletion(Duration.ofMinutes(1)).count()).isEqualTo(20);
        assertThat(consumed.getRecords())
                .extracting(ConsumerRecord::value)
                .contains(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19);
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
            return Multi.createFrom().range(0, 10);
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
            return Multi.createFrom().range(0, 10);
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
            return Multi.createFrom().range(0, 10);
        }

    }

    @ApplicationScoped
    public static class BeanProducingKafkaRecordNoValueNoKey {

        @SuppressWarnings("unused")
        @Incoming("data")
        @Outgoing("output-record")
        public Record<Integer, String> process(int input) {
            return Record.of(null, null);
        }

        @Outgoing("data")
        public Publisher<Integer> source() {
            return Multi.createFrom().range(0, 10);
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
            return Multi.createFrom().range(0, 10);
        }

    }

    @ApplicationScoped
    public static class BeanWithMultipleUpstreams {

        @Outgoing("data")
        public Publisher<Integer> source() {
            return Multi.createFrom().range(0, 10);
        }

        @Outgoing("data")
        public Publisher<Integer> source2() {
            return Multi.createFrom().range(10, 20)
                    .onItem().call(x -> Uni.createFrom().voidItem().onItem().delayIt().by(Duration.ofMillis(20)));
        }

    }

}
