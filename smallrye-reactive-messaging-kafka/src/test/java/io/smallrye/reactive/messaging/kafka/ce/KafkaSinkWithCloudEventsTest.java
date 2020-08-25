package io.smallrye.reactive.messaging.kafka.ce;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.net.URI;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Instance;
import javax.enterprise.inject.spi.BeanManager;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.DoubleSerializer;
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
import org.reactivestreams.Subscriber;

import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.ce.OutgoingCloudEventMetadata;
import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.kafka.*;
import io.smallrye.reactive.messaging.kafka.impl.KafkaSink;
import io.vertx.core.json.JsonObject;

public class KafkaSinkWithCloudEventsTest extends KafkaTestBase {

    private WeldContainer container;

    @After
    public void cleanup() {
        if (container != null) {
            container.close();
        }
        // Release the config objects
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    public void testSendingStructuredCloudEvents() {
        KafkaUsage usage = new KafkaUsage();
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = newCommonConfig();
        config.put("topic", topic);
        config.put("value.serializer", StringSerializer.class.getName());
        config.put("bootstrap.servers", SERVERS);
        config.put("channel-name", topic);
        config.put("cloud-events-mode", "structured");
        KafkaConnectorOutgoingConfiguration oc = new KafkaConnectorOutgoingConfiguration(new MapBasedConfig(config));
        KafkaSink sink = new KafkaSink(vertx, oc);

        Deserializer<String> keyDes = new StringDeserializer();
        String randomId = UUID.randomUUID().toString();

        List<ConsumerRecord<String, String>> records = new CopyOnWriteArrayList<>();
        usage.consume(randomId, randomId, OffsetResetStrategy.EARLIEST, keyDes, keyDes, () -> true, null,
                null, Collections.singletonList(topic), records::add);

        Message<?> message = Message.of("hello").addMetadata(OutgoingCloudEventMetadata.builder()
                .withSource(URI.create("test://test"))
                .withType("type")
                .withId("some id")
                .build());

        Multi.createFrom().<Message<?>> item(message)
                .subscribe().withSubscriber((Subscriber) sink.getSink().build());

        await().until(() -> records.size() == 1);

        ConsumerRecord<String, String> record = records.get(0);
        assertThat(record.topic()).isEqualTo(topic);
        assertThat(record.key()).isNull();
        assertThat(record.headers())
                .contains(new RecordHeader("content-type", "application/cloudevents+json; charset=UTF-8".getBytes()));
        JsonObject json = new JsonObject(record.value());
        assertThat(json.getString("specversion")).isEqualTo("1.0");
        assertThat(json.getString("type")).isEqualTo("type");
        assertThat(json.getString("source")).isEqualTo("test://test");
        assertThat(json.getString("id")).isEqualTo("some id");
        assertThat(json.getString("data")).isEqualTo("hello");
    }

    public static class Pet {
        public String name;
        public String kind;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    public void testSendingStructuredCloudEventsWithComplexPayload() {
        KafkaUsage usage = new KafkaUsage();
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = newCommonConfig();
        config.put("topic", topic);
        config.put("value.serializer", StringSerializer.class.getName());
        config.put("bootstrap.servers", SERVERS);
        config.put("channel-name", topic);
        config.put("cloud-events-mode", "structured");
        KafkaConnectorOutgoingConfiguration oc = new KafkaConnectorOutgoingConfiguration(new MapBasedConfig(config));
        KafkaSink sink = new KafkaSink(vertx, oc);

        Deserializer<String> keyDes = new StringDeserializer();
        String randomId = UUID.randomUUID().toString();

        List<ConsumerRecord<String, String>> records = new CopyOnWriteArrayList<>();
        usage.consume(randomId, randomId, OffsetResetStrategy.EARLIEST, keyDes, keyDes, () -> true, null,
                null, Collections.singletonList(topic), records::add);

        Pet neo = new Pet();
        neo.name = "neo";
        neo.kind = "rabbit";
        Message<?> message = Message.of(neo).addMetadata(OutgoingCloudEventMetadata.builder()
                .withSource(URI.create("test://test"))
                .withType("type")
                .withId("some id")
                .build());

        Multi.createFrom().<Message<?>> item(message)
                .subscribe().withSubscriber((Subscriber) sink.getSink().build());

        await().until(() -> records.size() == 1);

        ConsumerRecord<String, String> record = records.get(0);
        assertThat(record.topic()).isEqualTo(topic);
        assertThat(record.key()).isNull();
        assertThat(record.headers())
                .contains(new RecordHeader("content-type", "application/cloudevents+json; charset=UTF-8".getBytes()));
        JsonObject json = new JsonObject(record.value());
        assertThat(json.getString("specversion")).isEqualTo("1.0");
        assertThat(json.getString("type")).isEqualTo("type");
        assertThat(json.getString("source")).isEqualTo("test://test");
        assertThat(json.getString("id")).isEqualTo("some id");
        assertThat(json.getJsonObject("data").getString("name")).isEqualTo("neo");
        assertThat(json.getJsonObject("data").getString("kind")).isEqualTo("rabbit");
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    public void testSendingStructuredCloudEventsWithTimestampAndSubject() {
        KafkaUsage usage = new KafkaUsage();
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = newCommonConfig();
        config.put("topic", topic);
        config.put("value.serializer", StringSerializer.class.getName());
        config.put("bootstrap.servers", SERVERS);
        config.put("channel-name", topic);
        config.put("cloud-events-mode", "structured");
        KafkaConnectorOutgoingConfiguration oc = new KafkaConnectorOutgoingConfiguration(new MapBasedConfig(config));
        KafkaSink sink = new KafkaSink(vertx, oc);

        Deserializer<String> keyDes = new StringDeserializer();
        String randomId = UUID.randomUUID().toString();

        List<ConsumerRecord<String, String>> records = new CopyOnWriteArrayList<>();
        usage.consume(randomId, randomId, OffsetResetStrategy.EARLIEST, keyDes, keyDes, () -> true, null,
                null, Collections.singletonList(topic), records::add);

        ZonedDateTime time = ZonedDateTime.now();

        Message<?> message = Message.of("").addMetadata(OutgoingCloudEventMetadata.builder()
                .withSource(URI.create("test://test"))
                .withType("type")
                .withId("some id")
                .withSubject("subject")
                .withTimestamp(time)
                .build());

        Multi.createFrom().<Message<?>> item(message)
                .subscribe().withSubscriber((Subscriber) sink.getSink().build());

        await().until(() -> records.size() == 1);

        ConsumerRecord<String, String> record = records.get(0);
        assertThat(record.topic()).isEqualTo(topic);
        assertThat(record.key()).isNull();
        assertThat(record.headers())
                .contains(new RecordHeader("content-type", "application/cloudevents+json; charset=UTF-8".getBytes()));
        JsonObject json = new JsonObject(record.value());
        assertThat(json.getString("specversion")).isEqualTo("1.0");
        assertThat(json.getString("type")).isEqualTo("type");
        assertThat(json.getString("source")).isEqualTo("test://test");
        assertThat(json.getString("id")).isEqualTo("some id");
        assertThat(json.getString("subject")).isEqualTo("subject");
        assertThat(json.getInstant("time")).isNotNull();
        assertThat(json.getInstant("time").getEpochSecond()).isEqualTo(time.toEpochSecond());
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    public void testSendingStructuredCloudEventsMissingMandatoryAttribute() {
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = newCommonConfig();
        config.put("topic", topic);
        config.put("value.serializer", StringSerializer.class.getName());
        config.put("bootstrap.servers", SERVERS);
        config.put("channel-name", topic);
        config.put("cloud-events-mode", "structured");
        KafkaConnectorOutgoingConfiguration oc = new KafkaConnectorOutgoingConfiguration(new MapBasedConfig(config));
        KafkaSink sink = new KafkaSink(vertx, oc);

        Message<?> message = Message.of("hello").addMetadata(OutgoingCloudEventMetadata.builder()
                .withSource(URI.create("test://test"))
                // type omitted on purpose
                .withId("some id")
                .build());

        await().until(() -> {
            HealthReport.HealthReportBuilder builder = HealthReport.builder();
            sink.isAlive(builder);
            return builder.build().isOk();
        });

        Multi.createFrom().<Message<?>> item(message)
                .subscribe().withSubscriber((Subscriber) sink.getSink().build());

        await().until(() -> {
            HealthReport.HealthReportBuilder builder = HealthReport.builder();
            sink.isAlive(builder);
            return !builder.build().isOk();
        });
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test(expected = IllegalStateException.class)
    public void testSendingStructuredCloudEventsWithWrongSerializer() {
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = newCommonConfig();
        config.put("topic", topic);
        config.put("value.serializer", DoubleSerializer.class.getName());
        config.put("bootstrap.servers", SERVERS);
        config.put("channel-name", topic);
        config.put("cloud-events-mode", "structured");
        KafkaConnectorOutgoingConfiguration oc = new KafkaConnectorOutgoingConfiguration(new MapBasedConfig(config));
        new KafkaSink(vertx, oc);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    public void testSendingStructuredCloudEventsWithKey() {
        KafkaUsage usage = new KafkaUsage();
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = newCommonConfig();
        config.put("topic", topic);
        config.put("value.serializer", StringSerializer.class.getName());
        config.put("bootstrap.servers", SERVERS);
        config.put("channel-name", topic);
        config.put("cloud-events-mode", "structured");
        KafkaConnectorOutgoingConfiguration oc = new KafkaConnectorOutgoingConfiguration(new MapBasedConfig(config));
        KafkaSink sink = new KafkaSink(vertx, oc);

        Deserializer<String> keyDes = new StringDeserializer();
        String randomId = UUID.randomUUID().toString();

        List<ConsumerRecord<String, String>> records = new CopyOnWriteArrayList<>();
        usage.consume(randomId, randomId, OffsetResetStrategy.EARLIEST, keyDes, keyDes, () -> true, null,
                null, Collections.singletonList(topic), records::add);

        Message<?> message = Message.of("hello").addMetadata(OutgoingCloudEventMetadata.builder()
                .withSource(URI.create("test://test"))
                .withType("type")
                .withId("some id")
                .withExtension("partitionkey", "my-key")
                .build());

        Multi.createFrom().<Message<?>> item(message)
                .subscribe().withSubscriber((Subscriber) sink.getSink().build());

        await().until(() -> records.size() == 1);

        ConsumerRecord<String, String> record = records.get(0);
        assertThat(record.topic()).isEqualTo(topic);
        assertThat(record.key()).isEqualTo("my-key");
        assertThat(record.headers())
                .contains(new RecordHeader("content-type", "application/cloudevents+json; charset=UTF-8".getBytes()));
        JsonObject json = new JsonObject(record.value());
        assertThat(json.getString("specversion")).isEqualTo("1.0");
        assertThat(json.getString("type")).isEqualTo("type");
        assertThat(json.getString("source")).isEqualTo("test://test");
        assertThat(json.getString("id")).isEqualTo("some id");
        assertThat(json.getString("partitionkey")).isEqualTo("my-key"); // Rule 3.1 - partitionkey must be kept
        assertThat(json.getString("data")).isEqualTo("hello");
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    public void testSendingStructuredCloudEventsWithConfiguredTypeAndSource() {
        KafkaUsage usage = new KafkaUsage();
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = newCommonConfig();
        config.put("topic", topic);
        config.put("value.serializer", StringSerializer.class.getName());
        config.put("bootstrap.servers", SERVERS);
        config.put("channel-name", topic);
        config.put("cloud-events-mode", "structured");
        config.put("cloud-events-type", "my type");
        config.put("cloud-events-source", "http://acme.org");
        KafkaConnectorOutgoingConfiguration oc = new KafkaConnectorOutgoingConfiguration(new MapBasedConfig(config));
        KafkaSink sink = new KafkaSink(vertx, oc);

        Deserializer<String> keyDes = new StringDeserializer();
        String randomId = UUID.randomUUID().toString();

        List<ConsumerRecord<String, String>> records = new CopyOnWriteArrayList<>();
        usage.consume(randomId, randomId, OffsetResetStrategy.EARLIEST, keyDes, keyDes, () -> true, null,
                null, Collections.singletonList(topic), records::add);

        Message<?> message = Message.of("hello!").addMetadata(OutgoingCloudEventMetadata.builder()
                .withId("some id")
                .build());

        Multi.createFrom().<Message<?>> item(message)
                .subscribe().withSubscriber((Subscriber) sink.getSink().build());

        await().until(() -> records.size() == 1);

        ConsumerRecord<String, String> record = records.get(0);
        assertThat(record.topic()).isEqualTo(topic);
        assertThat(record.key()).isNull();
        assertThat(record.headers())
                .contains(new RecordHeader("content-type", "application/cloudevents+json; charset=UTF-8".getBytes()));
        JsonObject json = new JsonObject(record.value());
        assertThat(json.getString("specversion")).isEqualTo("1.0");
        assertThat(json.getString("type")).isEqualTo("my type");
        assertThat(json.getString("source")).isEqualTo("http://acme.org");
        assertThat(json.getString("id")).isEqualTo("some id");
        assertThat(json.getString("data")).isEqualTo("hello!");
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    public void testSendingStructuredCloudEventsWithConfiguredTypeAndSourceAndNoCloudEventMetadata() {
        KafkaUsage usage = new KafkaUsage();
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = newCommonConfig();
        config.put("topic", topic);
        config.put("value.serializer", StringSerializer.class.getName());
        config.put("bootstrap.servers", SERVERS);
        config.put("channel-name", topic);
        config.put("cloud-events-mode", "structured");
        config.put("cloud-events-type", "my type");
        config.put("cloud-events-source", "http://acme.org");
        KafkaConnectorOutgoingConfiguration oc = new KafkaConnectorOutgoingConfiguration(new MapBasedConfig(config));
        KafkaSink sink = new KafkaSink(vertx, oc);

        Deserializer<String> keyDes = new StringDeserializer();
        String randomId = UUID.randomUUID().toString();

        List<ConsumerRecord<String, String>> records = new CopyOnWriteArrayList<>();
        usage.consume(randomId, randomId, OffsetResetStrategy.EARLIEST, keyDes, keyDes, () -> true, null,
                null, Collections.singletonList(topic), records::add);

        Message<?> message = Message.of("hello!");

        Multi.createFrom().<Message<?>> item(message)
                .subscribe().withSubscriber((Subscriber) sink.getSink().build());

        await().until(() -> records.size() == 1);

        ConsumerRecord<String, String> record = records.get(0);
        assertThat(record.topic()).isEqualTo(topic);
        assertThat(record.key()).isNull();
        assertThat(record.headers())
                .contains(new RecordHeader("content-type", "application/cloudevents+json; charset=UTF-8".getBytes()));
        JsonObject json = new JsonObject(record.value());
        assertThat(json.getString("specversion")).isEqualTo("1.0");
        assertThat(json.getString("type")).isEqualTo("my type");
        assertThat(json.getString("source")).isEqualTo("http://acme.org");
        assertThat(json.getString("id")).isNotNull();
        assertThat(json.getString("data")).isEqualTo("hello!");
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    public void testSendingStructuredCloudEventsWithExtensions() {
        KafkaUsage usage = new KafkaUsage();
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = newCommonConfig();
        config.put("topic", topic);
        config.put("value.serializer", StringSerializer.class.getName());
        config.put("bootstrap.servers", SERVERS);
        config.put("channel-name", topic);
        config.put("cloud-events-mode", "structured");
        KafkaConnectorOutgoingConfiguration oc = new KafkaConnectorOutgoingConfiguration(new MapBasedConfig(config));
        KafkaSink sink = new KafkaSink(vertx, oc);

        Deserializer<String> keyDes = new StringDeserializer();
        String randomId = UUID.randomUUID().toString();

        List<ConsumerRecord<String, String>> records = new CopyOnWriteArrayList<>();
        usage.consume(randomId, randomId, OffsetResetStrategy.EARLIEST, keyDes, keyDes, () -> true, null,
                null, Collections.singletonList(topic), records::add);

        Message<?> message = Message.of("hello").addMetadata(OutgoingCloudEventMetadata.builder()
                .withSource(URI.create("test://test"))
                .withType("type")
                .withId("some id")
                .withExtension("ext", 123)
                .withExtension("ext2", "dddd")
                .build());

        Multi.createFrom().<Message<?>> item(message)
                .subscribe().withSubscriber((Subscriber) sink.getSink().build());

        await().until(() -> records.size() == 1);

        ConsumerRecord<String, String> record = records.get(0);
        assertThat(record.topic()).isEqualTo(topic);
        assertThat(record.key()).isNull();
        assertThat(record.headers())
                .contains(new RecordHeader("content-type", "application/cloudevents+json; charset=UTF-8".getBytes()));
        JsonObject json = new JsonObject(record.value());
        assertThat(json.getString("specversion")).isEqualTo("1.0");
        assertThat(json.getString("type")).isEqualTo("type");
        assertThat(json.getString("source")).isEqualTo("test://test");
        assertThat(json.getString("id")).isEqualTo("some id");
        assertThat(json.getString("ext2")).isEqualTo("dddd");
        assertThat(json.getInteger("ext")).isEqualTo(123);
        assertThat(json.getString("data")).isEqualTo("hello");
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    public void testSendingBinaryCloudEvents() {
        KafkaUsage usage = new KafkaUsage();
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = newCommonConfig();
        config.put("topic", topic);
        config.put("value.serializer", StringSerializer.class.getName());
        config.put("bootstrap.servers", SERVERS);
        config.put("channel-name", topic);
        KafkaConnectorOutgoingConfiguration oc = new KafkaConnectorOutgoingConfiguration(new MapBasedConfig(config));
        KafkaSink sink = new KafkaSink(vertx, oc);

        Deserializer<String> keyDes = new StringDeserializer();
        String randomId = UUID.randomUUID().toString();

        List<ConsumerRecord<String, String>> records = new CopyOnWriteArrayList<>();
        usage.consume(randomId, randomId, OffsetResetStrategy.EARLIEST, keyDes, keyDes, () -> true, null,
                null, Collections.singletonList(topic), records::add);

        Message<?> message = Message.of("hello").addMetadata(OutgoingCloudEventMetadata.builder()
                .withSource(URI.create("test://test"))
                .withType("type")
                .withId("some id")
                .build());

        Multi.createFrom().<Message<?>> item(message)
                .subscribe().withSubscriber((Subscriber) sink.getSink().build());

        await().until(() -> records.size() == 1);

        ConsumerRecord<String, String> record = records.get(0);
        assertThat(record.topic()).isEqualTo(topic);
        assertThat(record.key()).isNull();
        assertThat(record.headers())
                .contains(
                        new RecordHeader("ce_specversion", "1.0".getBytes()),
                        new RecordHeader("ce_type", "type".getBytes()),
                        new RecordHeader("ce_source", "test://test".getBytes()),
                        new RecordHeader("ce_id", "some id".getBytes()));
        assertThat(record.value()).isEqualTo("hello");
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    public void testSendingBinaryCloudEventsWithContentType() {
        KafkaUsage usage = new KafkaUsage();
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = newCommonConfig();
        config.put("topic", topic);
        config.put("value.serializer", StringSerializer.class.getName());
        config.put("bootstrap.servers", SERVERS);
        config.put("channel-name", topic);
        KafkaConnectorOutgoingConfiguration oc = new KafkaConnectorOutgoingConfiguration(new MapBasedConfig(config));
        KafkaSink sink = new KafkaSink(vertx, oc);

        Deserializer<String> keyDes = new StringDeserializer();
        String randomId = UUID.randomUUID().toString();

        List<ConsumerRecord<String, String>> records = new CopyOnWriteArrayList<>();
        usage.consume(randomId, randomId, OffsetResetStrategy.EARLIEST, keyDes, keyDes, () -> true, null,
                null, Collections.singletonList(topic), records::add);

        Message<?> message = Message.of("hello").addMetadata(OutgoingCloudEventMetadata.builder()
                .withSource(URI.create("test://test"))
                .withType("type")
                .withId("some id")
                .withDataContentType("text/plain")
                .build());

        Multi.createFrom().<Message<?>> item(message)
                .subscribe().withSubscriber((Subscriber) sink.getSink().build());

        await().until(() -> records.size() == 1);

        ConsumerRecord<String, String> record = records.get(0);
        assertThat(record.topic()).isEqualTo(topic);
        assertThat(record.key()).isNull();
        assertThat(record.headers())
                .contains(
                        new RecordHeader("ce_specversion", "1.0".getBytes()),
                        new RecordHeader("ce_type", "type".getBytes()),
                        // Rules 3.2.1
                        new RecordHeader("ce_datacontenttype", "text/plain".getBytes()),
                        new RecordHeader("content-type", "text/plain".getBytes()),
                        new RecordHeader("ce_source", "test://test".getBytes()),
                        new RecordHeader("ce_id", "some id".getBytes()));
        assertThat(record.value()).isEqualTo("hello");
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    public void testSendingBinaryCloudEventsWithKey() {
        KafkaUsage usage = new KafkaUsage();
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = newCommonConfig();
        config.put("topic", topic);
        config.put("value.serializer", StringSerializer.class.getName());
        config.put("bootstrap.servers", SERVERS);
        config.put("channel-name", topic);
        KafkaConnectorOutgoingConfiguration oc = new KafkaConnectorOutgoingConfiguration(new MapBasedConfig(config));
        KafkaSink sink = new KafkaSink(vertx, oc);

        Deserializer<String> keyDes = new StringDeserializer();
        String randomId = UUID.randomUUID().toString();

        List<ConsumerRecord<String, String>> records = new CopyOnWriteArrayList<>();
        usage.consume(randomId, randomId, OffsetResetStrategy.EARLIEST, keyDes, keyDes, () -> true, null,
                null, Collections.singletonList(topic), records::add);

        Message<?> message = Message.of("hello").addMetadata(OutgoingCloudEventMetadata.builder()
                .withSource(URI.create("test://test"))
                .withType("type")
                .withId("some id")
                .withExtension("partitionkey", "my-key")
                .build());

        Multi.createFrom().<Message<?>> item(message)
                .subscribe().withSubscriber((Subscriber) sink.getSink().build());

        await().until(() -> records.size() == 1);

        ConsumerRecord<String, String> record = records.get(0);
        assertThat(record.topic()).isEqualTo(topic);
        assertThat(record.key()).isEqualTo("my-key");
        assertThat(record.headers())
                .contains(
                        new RecordHeader("ce_specversion", "1.0".getBytes()),
                        new RecordHeader("ce_type", "type".getBytes()),
                        new RecordHeader("ce_source", "test://test".getBytes()),
                        new RecordHeader("ce_partitionkey", "my-key".getBytes()),
                        new RecordHeader("ce_id", "some id".getBytes()));
        assertThat(record.value()).isEqualTo("hello");
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    public void testSendingBinaryCloudEventsWithConfiguredTypeAndSource() {
        KafkaUsage usage = new KafkaUsage();
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = newCommonConfig();
        config.put("topic", topic);
        config.put("value.serializer", StringSerializer.class.getName());
        config.put("bootstrap.servers", SERVERS);
        config.put("channel-name", topic);
        config.put("key", "my-key");
        config.put("cloud-events-type", "my type");
        config.put("cloud-events-source", "http://acme.org");
        KafkaConnectorOutgoingConfiguration oc = new KafkaConnectorOutgoingConfiguration(new MapBasedConfig(config));
        KafkaSink sink = new KafkaSink(vertx, oc);

        Deserializer<String> keyDes = new StringDeserializer();
        String randomId = UUID.randomUUID().toString();

        List<ConsumerRecord<String, String>> records = new CopyOnWriteArrayList<>();
        usage.consume(randomId, randomId, OffsetResetStrategy.EARLIEST, keyDes, keyDes, () -> true, null,
                null, Collections.singletonList(topic), records::add);

        Message<?> message = Message.of("hello!").addMetadata(OutgoingCloudEventMetadata.builder()
                .withId("some id")
                .build());

        Multi.createFrom().<Message<?>> item(message)
                .subscribe().withSubscriber((Subscriber) sink.getSink().build());

        await().until(() -> records.size() == 1);

        ConsumerRecord<String, String> record = records.get(0);
        assertThat(record.topic()).isEqualTo(topic);
        assertThat(record.key()).isEqualTo("my-key");
        assertThat(record.headers())
                .contains(
                        new RecordHeader("ce_specversion", "1.0".getBytes()),
                        new RecordHeader("ce_type", "my type".getBytes()),
                        new RecordHeader("ce_source", "http://acme.org".getBytes()),
                        new RecordHeader("ce_id", "some id".getBytes()));
        assertThat(record.value()).isEqualTo("hello!");
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    public void testSendingBinaryCloudEventsWithConfiguredTypeAndSourceButNoMetadata() {
        KafkaUsage usage = new KafkaUsage();
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = newCommonConfig();
        config.put("topic", topic);
        config.put("value.serializer", StringSerializer.class.getName());
        config.put("bootstrap.servers", SERVERS);
        config.put("channel-name", topic);
        config.put("key", "my-key");
        config.put("cloud-events-type", "my type");
        config.put("cloud-events-source", "http://acme.org");
        KafkaConnectorOutgoingConfiguration oc = new KafkaConnectorOutgoingConfiguration(new MapBasedConfig(config));
        KafkaSink sink = new KafkaSink(vertx, oc);

        Deserializer<String> keyDes = new StringDeserializer();
        String randomId = UUID.randomUUID().toString();

        List<ConsumerRecord<String, String>> records = new CopyOnWriteArrayList<>();
        usage.consume(randomId, randomId, OffsetResetStrategy.EARLIEST, keyDes, keyDes, () -> true, null,
                null, Collections.singletonList(topic), records::add);

        Message<?> message = Message.of("hello!");

        Multi.createFrom().<Message<?>> item(message)
                .subscribe().withSubscriber((Subscriber) sink.getSink().build());

        await().until(() -> records.size() == 1);

        ConsumerRecord<String, String> record = records.get(0);
        assertThat(record.topic()).isEqualTo(topic);
        assertThat(record.key()).isEqualTo("my-key");
        assertThat(record.headers())
                .contains(
                        new RecordHeader("ce_specversion", "1.0".getBytes()),
                        new RecordHeader("ce_type", "my type".getBytes()),
                        new RecordHeader("ce_source", "http://acme.org".getBytes()));
        assertThat(record.headers().lastHeader("ce_id")).isNotNull();
        assertThat(record.value()).isEqualTo("hello!");
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    public void testSendingBinaryCloudEventsMissingMandatoryAttribute() {
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = newCommonConfig();
        config.put("topic", topic);
        config.put("value.serializer", StringSerializer.class.getName());
        config.put("bootstrap.servers", SERVERS);
        config.put("channel-name", topic);
        KafkaConnectorOutgoingConfiguration oc = new KafkaConnectorOutgoingConfiguration(new MapBasedConfig(config));
        KafkaSink sink = new KafkaSink(vertx, oc);

        Message<?> message = Message.of("hello").addMetadata(OutgoingCloudEventMetadata.builder()
                .withSource(URI.create("test://test"))
                // type omitted on purpose
                .withId("some id")
                .build());

        await().until(() -> {
            HealthReport.HealthReportBuilder builder = HealthReport.builder();
            sink.isAlive(builder);
            return builder.build().isOk();
        });

        Multi.createFrom().<Message<?>> item(message)
                .subscribe().withSubscriber((Subscriber) sink.getSink().build());

        await().until(() -> {
            HealthReport.HealthReportBuilder builder = HealthReport.builder();
            sink.isAlive(builder);
            return !builder.build().isOk();
        });
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    public void testWithCloudEventDisabled() {
        KafkaUsage usage = new KafkaUsage();
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = newCommonConfig();
        config.put("topic", topic);
        config.put("value.serializer", StringSerializer.class.getName());
        config.put("bootstrap.servers", SERVERS);
        config.put("channel-name", topic);
        config.put("key", "my-key");
        config.put("cloud-events", false);
        KafkaConnectorOutgoingConfiguration oc = new KafkaConnectorOutgoingConfiguration(new MapBasedConfig(config));
        KafkaSink sink = new KafkaSink(vertx, oc);

        Deserializer<String> keyDes = new StringDeserializer();
        String randomId = UUID.randomUUID().toString();

        List<ConsumerRecord<String, String>> records = new CopyOnWriteArrayList<>();
        usage.consume(randomId, randomId, OffsetResetStrategy.EARLIEST, keyDes, keyDes, () -> true, null,
                null, Collections.singletonList(topic), records::add);

        Message<?> message = Message.of("hello!").addMetadata(OutgoingCloudEventMetadata.builder()
                .withId("some id")
                .build());

        Multi.createFrom().<Message<?>> item(message)
                .subscribe().withSubscriber((Subscriber) sink.getSink().build());

        await().until(() -> records.size() == 1);

        ConsumerRecord<String, String> record = records.get(0);
        assertThat(record.topic()).isEqualTo(topic);
        assertThat(record.key()).isEqualTo("my-key");
        assertThat(record.headers().lastHeader("ce_specversion")).isNull();
        assertThat(record.headers().lastHeader("ce_id")).isNull();
        assertThat(record.value()).isEqualTo("hello!");
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    public void testSendingBinaryCloudEventsWithExtensions() {
        KafkaUsage usage = new KafkaUsage();
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = newCommonConfig();
        config.put("topic", topic);
        config.put("value.serializer", StringSerializer.class.getName());
        config.put("bootstrap.servers", SERVERS);
        config.put("channel-name", topic);
        KafkaConnectorOutgoingConfiguration oc = new KafkaConnectorOutgoingConfiguration(new MapBasedConfig(config));
        KafkaSink sink = new KafkaSink(vertx, oc);

        Deserializer<String> keyDes = new StringDeserializer();
        String randomId = UUID.randomUUID().toString();

        List<ConsumerRecord<String, String>> records = new CopyOnWriteArrayList<>();
        usage.consume(randomId, randomId, OffsetResetStrategy.EARLIEST, keyDes, keyDes, () -> true, null,
                null, Collections.singletonList(topic), records::add);

        Message<?> message = Message.of("hello").addMetadata(OutgoingCloudEventMetadata.builder()
                .withSource(URI.create("test://test"))
                .withType("type")
                .withId("some id")
                .withExtension("ext", 124)
                .withExtension("ext2", "bonjour")
                .build());

        Multi.createFrom().<Message<?>> item(message)
                .subscribe().withSubscriber((Subscriber) sink.getSink().build());

        await().until(() -> records.size() == 1);

        ConsumerRecord<String, String> record = records.get(0);
        assertThat(record.topic()).isEqualTo(topic);
        assertThat(record.key()).isNull();
        assertThat(record.headers())
                .contains(
                        new RecordHeader("ce_specversion", "1.0".getBytes()),
                        new RecordHeader("ce_type", "type".getBytes()),
                        new RecordHeader("ce_source", "test://test".getBytes()),
                        new RecordHeader("ce_id", "some id".getBytes()),
                        new RecordHeader("ce_ext", "124".getBytes()),
                        new RecordHeader("ce_ext2", "bonjour".getBytes()));
        assertThat(record.value()).isEqualTo("hello");
    }

    @Test
    public void testSendingStructuredCloudEventFromBean() {
        String topic = UUID.randomUUID().toString();

        KafkaUsage usage = new KafkaUsage();
        Deserializer<String> keyDes = new StringDeserializer();
        String randomId = UUID.randomUUID().toString();
        List<ConsumerRecord<String, String>> records = new CopyOnWriteArrayList<>();
        usage.consume(randomId, randomId, OffsetResetStrategy.EARLIEST, keyDes, keyDes, () -> true, null,
                null, Collections.singletonList(topic), records::add);

        deploy(getConfigToSendStructuredCloudEvents(topic));

        await().until(() -> records.size() >= 10);

        assertThat(records).allSatisfy(record -> {
            assertThat(record.topic()).isEqualTo(topic);
            assertThat(record.key()).isNull();
            assertThat(record.headers())
                    .contains(new RecordHeader("content-type", "application/cloudevents+json; charset=UTF-8".getBytes()));
            JsonObject json = new JsonObject(record.value());
            assertThat(json.getString("specversion")).isEqualTo("1.0");
            assertThat(json.getString("type")).isEqualTo("greeting");
            assertThat(json.getString("source")).isEqualTo("source://me");
            assertThat(json.getString("id")).startsWith("id-hello-");
            assertThat(json.getString("data")).startsWith("hello-");
        });
    }

    @Test
    public void testSendingBinaryCloudEventFromBean() {
        String topic = UUID.randomUUID().toString();

        KafkaUsage usage = new KafkaUsage();
        Deserializer<String> keyDes = new StringDeserializer();
        String randomId = UUID.randomUUID().toString();
        List<ConsumerRecord<String, String>> records = new CopyOnWriteArrayList<>();
        usage.consume(randomId, randomId, OffsetResetStrategy.EARLIEST, keyDes, keyDes, () -> true, null,
                null, Collections.singletonList(topic), records::add);

        deploy(getConfigToSendBinaryCloudEvents(topic));

        await().until(() -> records.size() >= 10);

        assertThat(records).allSatisfy(record -> {
            assertThat(record.topic()).isEqualTo(topic);
            assertThat(record.key()).isNull();
            assertThat(record.headers())
                    .contains(
                            new RecordHeader("ce_specversion", "1.0".getBytes()),
                            new RecordHeader("ce_type", "greeting".getBytes()),
                            new RecordHeader("ce_source", "source://me".getBytes()));
            assertThat(record.value()).startsWith("hello-");
            assertThat(new String(record.headers().lastHeader("ce_id").value())).startsWith("id-hello-");
        });
    }

    @Test
    public void testSendingBinaryCloudEventFromBeanWithDefault() {
        String topic = UUID.randomUUID().toString();

        KafkaUsage usage = new KafkaUsage();
        Deserializer<String> keyDes = new StringDeserializer();
        String randomId = UUID.randomUUID().toString();
        List<ConsumerRecord<String, String>> records = new CopyOnWriteArrayList<>();
        usage.consume(randomId, randomId, OffsetResetStrategy.EARLIEST, keyDes, keyDes, () -> true, null,
                null, Collections.singletonList(topic), records::add);

        deploySource(getConfigToSendBinaryCloudEventsWithDefault(topic));

        await().until(() -> records.size() >= 10);

        assertThat(records).allSatisfy(record -> {
            assertThat(record.topic()).isEqualTo(topic);
            assertThat(record.key()).isNull();
            assertThat(record.headers())
                    .contains(
                            new RecordHeader("ce_specversion", "1.0".getBytes()),
                            new RecordHeader("ce_type", "greetings".getBytes()),
                            new RecordHeader("ce_source", "source://me".getBytes()),
                            new RecordHeader("ce_subject", "test".getBytes()));
            assertThat(record.value()).startsWith("hello-");
            assertThat(new String(record.headers().lastHeader("ce_id").value())).isNotNull();
            assertThat(new String(record.headers().lastHeader("ce_time").value())).isNotNull();
        });
    }

    @Test
    public void testSendingStructuredCloudEventFromBeanWithDefault() {
        String topic = UUID.randomUUID().toString();

        KafkaUsage usage = new KafkaUsage();
        Deserializer<String> keyDes = new StringDeserializer();
        String randomId = UUID.randomUUID().toString();
        List<ConsumerRecord<String, String>> records = new CopyOnWriteArrayList<>();
        usage.consume(randomId, randomId, OffsetResetStrategy.EARLIEST, keyDes, keyDes, () -> true, null,
                null, Collections.singletonList(topic), records::add);

        deploySource(getConfigToSendStructuredCloudEventsWithDefault(topic));

        await().until(() -> records.size() >= 10);

        assertThat(records).allSatisfy(record -> {
            assertThat(record.topic()).isEqualTo(topic);
            assertThat(record.key()).isNull();
            assertThat(record.headers())
                    .contains(new RecordHeader("content-type", "application/cloudevents+json; charset=UTF-8".getBytes()));
            JsonObject json = new JsonObject(record.value());
            assertThat(json.getString("specversion")).isEqualTo("1.0");
            assertThat(json.getString("type")).isEqualTo("greetings");
            assertThat(json.getString("source")).isEqualTo("source://me");
            assertThat(json.getString("subject")).isEqualTo("test");
            assertThat(json.getString("id")).isNotNull();
            assertThat(json.getInstant("time")).isNotNull();
            assertThat(json.getString("data")).startsWith("hello-");
        });
    }

    private void deploy(MapBasedConfig config) {
        Weld weld = baseWeld();
        addConfig(config);
        weld.addBeanClass(Source.class);
        weld.addBeanClass(Processing.class);
        weld.addBeanClass(Sender.class);
        weld.addBeanClass(ConsumptionConsumerRebalanceListener.class);
        weld.disableDiscovery();
        container = weld.initialize();
    }

    private void deploySource(MapBasedConfig config) {
        Weld weld = baseWeld();
        addConfig(config);
        weld.addBeanClass(Source.class);
        weld.addBeanClass(ConsumptionConsumerRebalanceListener.class);
        weld.disableDiscovery();
        container = weld.initialize();
    }

    private MapBasedConfig getConfigToSendStructuredCloudEvents(String topic) {
        String prefix = "mp.messaging.outgoing.kafka.";
        Map<String, Object> config = new HashMap<>();
        config.put(prefix + "connector", KafkaConnector.CONNECTOR_NAME);
        config.put(prefix + "value.serializer", StringSerializer.class.getName());
        config.put(prefix + "cloud-events-mode", "structured");
        config.put(prefix + "topic", topic);
        return new MapBasedConfig(config);
    }

    private MapBasedConfig getConfigToSendBinaryCloudEvents(String topic) {
        String prefix = "mp.messaging.outgoing.kafka.";
        Map<String, Object> config = new HashMap<>();
        config.put(prefix + "connector", KafkaConnector.CONNECTOR_NAME);
        config.put(prefix + "value.serializer", StringSerializer.class.getName());
        config.put(prefix + "topic", topic);
        return new MapBasedConfig(config);
    }

    private MapBasedConfig getConfigToSendBinaryCloudEventsWithDefault(String topic) {
        String prefix = "mp.messaging.outgoing.source.";
        Map<String, Object> config = new HashMap<>();
        config.put(prefix + "connector", KafkaConnector.CONNECTOR_NAME);
        config.put(prefix + "value.serializer", StringSerializer.class.getName());
        config.put(prefix + "topic", topic);
        config.put(prefix + "cloud-events-type", "greetings");
        config.put(prefix + "cloud-events-source", "source://me");
        config.put(prefix + "cloud-events-subject", "test");
        return new MapBasedConfig(config);
    }

    private MapBasedConfig getConfigToSendStructuredCloudEventsWithDefault(String topic) {
        String prefix = "mp.messaging.outgoing.source.";
        Map<String, Object> config = new HashMap<>();
        config.put(prefix + "connector", KafkaConnector.CONNECTOR_NAME);
        config.put(prefix + "value.serializer", StringSerializer.class.getName());
        config.put(prefix + "topic", topic);
        config.put(prefix + "cloud-events-type", "greetings");
        config.put(prefix + "cloud-events-source", "source://me");
        config.put(prefix + "cloud-events-subject", "test");
        config.put(prefix + "cloud-events-mode", "structured");
        return new MapBasedConfig(config);
    }

    private Map<String, Object> newCommonConfig() {
        String randomId = UUID.randomUUID().toString();
        Map<String, Object> config = new HashMap<>();
        config.put("bootstrap.servers", "localhost:9092");
        config.put("group.id", randomId);
        config.put("key.serializer", StringSerializer.class.getName());
        config.put("enable.auto.commit", "false");
        config.put("auto.offset.reset", "earliest");
        return config;
    }

    private BeanManager getBeanManager() {
        if (container == null) {
            Weld weld = baseWeld();
            addConfig(new MapBasedConfig(new HashMap<>()));
            weld.disableDiscovery();
            container = weld.initialize();
        }
        return container.getBeanManager();
    }

    private Instance<KafkaConsumerRebalanceListener> getConsumerRebalanceListeners() {
        return getBeanManager()
                .createInstance()
                .select(KafkaConsumerRebalanceListener.class);
    }

    @ApplicationScoped
    public static class Source {

        @Outgoing("source")
        public Multi<String> source() {
            return Multi.createFrom().range(0, 10)
                    .map(i -> "hello-" + i);
        }
    }

    @ApplicationScoped
    public static class Processing {
        @Incoming("source")
        @Outgoing("processed")
        public Message<String> process(Message<String> in) {
            return in.addMetadata(OutgoingCloudEventMetadata.builder()
                    .withId("id-" + in.getPayload())
                    .withType("greeting")
                    .build());
        }
    }

    @ApplicationScoped
    public static class Sender {
        @SuppressWarnings("unchecked")
        @Incoming("processed")
        @Outgoing("kafka")
        public Message<String> process(Message<String> in) {
            OutgoingCloudEventMetadata<String> metadata = in
                    .getMetadata(OutgoingCloudEventMetadata.class)
                    .orElseThrow(() -> new IllegalStateException("Expected metadata to be present"));

            return in.addMetadata(OutgoingCloudEventMetadata.from(metadata)
                    .withSource(URI.create("source://me"))
                    .withSubject("test")
                    .build());
        }
    }

}
