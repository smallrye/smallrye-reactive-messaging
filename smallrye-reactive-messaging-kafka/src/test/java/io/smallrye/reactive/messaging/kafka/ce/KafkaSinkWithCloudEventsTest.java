package io.smallrye.reactive.messaging.kafka.ce;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.net.URI;
import java.time.ZonedDateTime;
import java.util.UUID;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.ce.OutgoingCloudEventMetadata;
import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.kafka.ConsumptionConsumerRebalanceListener;
import io.smallrye.reactive.messaging.kafka.CountKafkaCdiEvents;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorOutgoingConfiguration;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;
import io.smallrye.reactive.messaging.kafka.base.UnsatisfiedInstance;
import io.smallrye.reactive.messaging.kafka.companion.ConsumerTask;
import io.smallrye.reactive.messaging.kafka.impl.KafkaSink;
import io.vertx.core.json.JsonObject;

public class KafkaSinkWithCloudEventsTest extends KafkaCompanionTestBase {

    KafkaSink sink;

    @AfterEach
    public void stopAll() {
        if (sink != null) {
            sink.closeQuietly();
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    public void testSendingStructuredCloudEvents() {
        KafkaMapBasedConfig config = newCommonConfig();
        config.put("topic", topic);
        config.put("value.serializer", StringSerializer.class.getName());
        config.put("channel-name", topic);
        config.put("cloud-events-mode", "structured");
        KafkaConnectorOutgoingConfiguration oc = new KafkaConnectorOutgoingConfiguration(config);
        sink = new KafkaSink(oc, CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance());

        ConsumerTask<String, String> records = companion.consumeStrings().fromTopics(topic);

        Message<?> message = Message.of("hello").addMetadata(OutgoingCloudEventMetadata.builder()
                .withSource(URI.create("test://test"))
                .withType("type")
                .withId("some id")
                .build());

        Multi.createFrom().<Message<?>> item(message)
                .subscribe().withSubscriber((Subscriber) sink.getSink().build());

        await().until(() -> records.count() == 1);

        ConsumerRecord<String, String> record = records.getRecords().get(0);
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
        KafkaMapBasedConfig config = newCommonConfig();
        config.put("topic", topic);
        config.put("value.serializer", StringSerializer.class.getName());
        config.put("channel-name", topic);
        config.put("cloud-events-mode", "structured");
        KafkaConnectorOutgoingConfiguration oc = new KafkaConnectorOutgoingConfiguration(config);
        sink = new KafkaSink(oc, CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance());

        ConsumerTask<String, String> records = companion.consumeStrings().fromTopics(topic);

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

        await().until(() -> records.count() == 1);

        ConsumerRecord<String, String> record = records.getRecords().get(0);
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
        KafkaMapBasedConfig config = newCommonConfig();
        config.put("topic", topic);
        config.put("value.serializer", StringSerializer.class.getName());
        config.put("channel-name", topic);
        config.put("cloud-events-mode", "structured");
        KafkaConnectorOutgoingConfiguration oc = new KafkaConnectorOutgoingConfiguration(config);
        sink = new KafkaSink(oc, CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance());

        ConsumerTask<String, String> records = companion.consumeStrings().fromTopics(topic);

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

        await().until(() -> records.count() == 1);

        ConsumerRecord<String, String> record = records.getRecords().get(0);
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
        KafkaMapBasedConfig config = newCommonConfig();
        config.put("topic", topic);
        config.put("value.serializer", StringSerializer.class.getName());
        config.put("channel-name", topic);
        config.put("cloud-events-mode", "structured");
        KafkaConnectorOutgoingConfiguration oc = new KafkaConnectorOutgoingConfiguration(config);
        sink = new KafkaSink(oc, CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance());

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

    @Test
    public void testSendingStructuredCloudEventsWithWrongSerializer() {
        KafkaMapBasedConfig config = newCommonConfig();
        config.put("topic", topic);
        config.put("value.serializer", DoubleSerializer.class.getName());
        config.put("channel-name", topic);
        config.put("cloud-events-mode", "structured");
        KafkaConnectorOutgoingConfiguration oc = new KafkaConnectorOutgoingConfiguration(config);

        assertThatThrownBy(() -> new KafkaSink(oc, CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance()))
                .isInstanceOf(IllegalStateException.class);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    public void testSendingStructuredCloudEventsWithKey() {
        KafkaMapBasedConfig config = newCommonConfig();
        config.put("topic", topic);
        config.put("value.serializer", StringSerializer.class.getName());
        config.put("channel-name", topic);
        config.put("cloud-events-mode", "structured");
        KafkaConnectorOutgoingConfiguration oc = new KafkaConnectorOutgoingConfiguration(config);
        sink = new KafkaSink(oc, CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance());

        ConsumerTask<String, String> records = companion.consumeStrings().fromTopics(topic);

        Message<?> message = Message.of("hello").addMetadata(OutgoingCloudEventMetadata.builder()
                .withSource(URI.create("test://test"))
                .withType("type")
                .withId("some id")
                .withExtension("partitionkey", "my-key")
                .build());

        Multi.createFrom().<Message<?>> item(message)
                .subscribe().withSubscriber((Subscriber) sink.getSink().build());

        await().until(() -> records.count() == 1);

        ConsumerRecord<String, String> record = records.getRecords().get(0);
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
        KafkaMapBasedConfig config = newCommonConfig();
        config.put("topic", topic);
        config.put("value.serializer", StringSerializer.class.getName());
        config.put("channel-name", topic);
        config.put("cloud-events-mode", "structured");
        config.put("cloud-events-type", "my type");
        config.put("cloud-events-source", "http://acme.org");
        KafkaConnectorOutgoingConfiguration oc = new KafkaConnectorOutgoingConfiguration(config);
        sink = new KafkaSink(oc, CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance());

        ConsumerTask<String, String> records = companion.consumeStrings().fromTopics(topic);

        Message<?> message = Message.of("hello!").addMetadata(OutgoingCloudEventMetadata.builder()
                .withId("some id")
                .build());

        Multi.createFrom().<Message<?>> item(message)
                .subscribe().withSubscriber((Subscriber) sink.getSink().build());

        await().until(() -> records.count() == 1);

        ConsumerRecord<String, String> record = records.getRecords().get(0);
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
        KafkaMapBasedConfig config = newCommonConfig();
        config.put("topic", topic);
        config.put("value.serializer", StringSerializer.class.getName());
        config.put("channel-name", topic);
        config.put("cloud-events-mode", "structured");
        config.put("cloud-events-type", "my type");
        config.put("cloud-events-source", "http://acme.org");
        KafkaConnectorOutgoingConfiguration oc = new KafkaConnectorOutgoingConfiguration(config);
        sink = new KafkaSink(oc, CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance());

        ConsumerTask<String, String> records = companion.consumeStrings().fromTopics(topic);

        Message<?> message = Message.of("hello!");

        Multi.createFrom().<Message<?>> item(message)
                .subscribe().withSubscriber((Subscriber) sink.getSink().build());

        await().until(() -> records.getRecords().size() == 1);

        ConsumerRecord<String, String> record = records.getRecords().get(0);
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
        KafkaMapBasedConfig config = newCommonConfig();
        config.put("topic", topic);
        config.put("value.serializer", StringSerializer.class.getName());
        config.put("channel-name", topic);
        config.put("cloud-events-mode", "structured");
        KafkaConnectorOutgoingConfiguration oc = new KafkaConnectorOutgoingConfiguration(config);
        sink = new KafkaSink(oc, CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance());

        ConsumerTask<String, String> records = companion.consumeStrings().fromTopics(topic);

        Message<?> message = Message.of("hello").addMetadata(OutgoingCloudEventMetadata.builder()
                .withSource(URI.create("test://test"))
                .withType("type")
                .withId("some id")
                .withExtension("ext", 123)
                .withExtension("ext2", "dddd")
                .build());

        Multi.createFrom().<Message<?>> item(message)
                .subscribe().withSubscriber((Subscriber) sink.getSink().build());

        await().until(() -> records.getRecords().size() == 1);

        ConsumerRecord<String, String> record = records.getRecords().get(0);
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
        KafkaMapBasedConfig config = newCommonConfig();
        config.put("topic", topic);
        config.put("value.serializer", StringSerializer.class.getName());
        config.put("channel-name", topic);
        KafkaConnectorOutgoingConfiguration oc = new KafkaConnectorOutgoingConfiguration(config);
        sink = new KafkaSink(oc, CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance());

        ConsumerTask<String, String> records = companion.consumeStrings().fromTopics(topic);

        Message<?> message = Message.of("hello").addMetadata(OutgoingCloudEventMetadata.builder()
                .withSource(URI.create("test://test"))
                .withType("type")
                .withId("some id")
                .build());

        Multi.createFrom().<Message<?>> item(message)
                .subscribe().withSubscriber((Subscriber) sink.getSink().build());

        await().until(() -> records.getRecords().size() == 1);

        ConsumerRecord<String, String> record = records.getRecords().get(0);
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
        KafkaMapBasedConfig config = newCommonConfig();
        config.put("topic", topic);
        config.put("value.serializer", StringSerializer.class.getName());
        config.put("channel-name", topic);
        KafkaConnectorOutgoingConfiguration oc = new KafkaConnectorOutgoingConfiguration(config);
        sink = new KafkaSink(oc, CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance());

        ConsumerTask<String, String> records = companion.consumeStrings().fromTopics(topic);

        Message<?> message = Message.of("hello").addMetadata(OutgoingCloudEventMetadata.builder()
                .withSource(URI.create("test://test"))
                .withType("type")
                .withId("some id")
                .withDataContentType("text/plain")
                .build());

        Multi.createFrom().<Message<?>> item(message)
                .subscribe().withSubscriber((Subscriber) sink.getSink().build());

        await().until(() -> records.getRecords().size() == 1);

        ConsumerRecord<String, String> record = records.getRecords().get(0);
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
        KafkaMapBasedConfig config = newCommonConfig();
        config.put("topic", topic);
        config.put("value.serializer", StringSerializer.class.getName());
        config.put("channel-name", topic);
        KafkaConnectorOutgoingConfiguration oc = new KafkaConnectorOutgoingConfiguration(config);
        sink = new KafkaSink(oc, CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance());

        ConsumerTask<String, String> records = companion.consumeStrings().fromTopics(topic);

        Message<?> message = Message.of("hello").addMetadata(OutgoingCloudEventMetadata.builder()
                .withSource(URI.create("test://test"))
                .withType("type")
                .withId("some id")
                .withExtension("partitionkey", "my-key")
                .build());

        Multi.createFrom().<Message<?>> item(message)
                .subscribe().withSubscriber((Subscriber) sink.getSink().build());

        await().until(() -> records.getRecords().size() == 1);

        ConsumerRecord<String, String> record = records.getRecords().get(0);
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
        KafkaMapBasedConfig config = newCommonConfig();
        config.put("topic", topic);
        config.put("value.serializer", StringSerializer.class.getName());
        config.put("channel-name", topic);
        config.put("key", "my-key");
        config.put("cloud-events-type", "my type");
        config.put("cloud-events-source", "http://acme.org");
        KafkaConnectorOutgoingConfiguration oc = new KafkaConnectorOutgoingConfiguration(config);
        sink = new KafkaSink(oc, CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance());

        ConsumerTask<String, String> records = companion.consumeStrings().fromTopics(topic);

        Message<?> message = Message.of("hello!").addMetadata(OutgoingCloudEventMetadata.builder()
                .withId("some id")
                .build());

        Multi.createFrom().<Message<?>> item(message)
                .subscribe().withSubscriber((Subscriber) sink.getSink().build());

        await().until(() -> records.getRecords().size() == 1);

        ConsumerRecord<String, String> record = records.getRecords().get(0);
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
        KafkaMapBasedConfig config = newCommonConfig();
        config.put("topic", topic);
        config.put("value.serializer", StringSerializer.class.getName());
        config.put("channel-name", topic);
        config.put("key", "my-key");
        config.put("cloud-events-type", "my type");
        config.put("cloud-events-source", "http://acme.org");
        KafkaConnectorOutgoingConfiguration oc = new KafkaConnectorOutgoingConfiguration(config);
        sink = new KafkaSink(oc, CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance());

        ConsumerTask<String, String> records = companion.consumeStrings().fromTopics(topic);

        Message<?> message = Message.of("hello!");

        Multi.createFrom().<Message<?>> item(message)
                .subscribe().withSubscriber((Subscriber) sink.getSink().build());

        await().until(() -> records.getRecords().size() == 1);

        ConsumerRecord<String, String> record = records.getRecords().get(0);
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
        KafkaMapBasedConfig config = newCommonConfig();
        config.put("topic", topic);
        config.put("value.serializer", StringSerializer.class.getName());
        config.put("channel-name", topic);
        KafkaConnectorOutgoingConfiguration oc = new KafkaConnectorOutgoingConfiguration(config);
        sink = new KafkaSink(oc, CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance());

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
        KafkaMapBasedConfig config = newCommonConfig();
        config.put("topic", topic);
        config.put("value.serializer", StringSerializer.class.getName());
        config.put("channel-name", topic);
        config.put("key", "my-key");
        config.put("cloud-events", false);
        KafkaConnectorOutgoingConfiguration oc = new KafkaConnectorOutgoingConfiguration(config);
        sink = new KafkaSink(oc, CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance());

        ConsumerTask<String, String> records = companion.consumeStrings().fromTopics(topic);

        Message<?> message = Message.of("hello!").addMetadata(OutgoingCloudEventMetadata.builder()
                .withId("some id")
                .build());

        Multi.createFrom().<Message<?>> item(message)
                .subscribe().withSubscriber((Subscriber) sink.getSink().build());

        await().until(() -> records.getRecords().size() == 1);

        ConsumerRecord<String, String> record = records.getRecords().get(0);
        assertThat(record.topic()).isEqualTo(topic);
        assertThat(record.key()).isEqualTo("my-key");
        assertThat(record.headers().lastHeader("ce_specversion")).isNull();
        assertThat(record.headers().lastHeader("ce_id")).isNull();
        assertThat(record.value()).isEqualTo("hello!");
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    public void testSendingBinaryCloudEventsWithExtensions() {
        KafkaMapBasedConfig config = newCommonConfig();
        config.put("topic", topic);
        config.put("value.serializer", StringSerializer.class.getName());
        config.put("channel-name", topic);
        KafkaConnectorOutgoingConfiguration oc = new KafkaConnectorOutgoingConfiguration(config);
        sink = new KafkaSink(oc, CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance());

        ConsumerTask<String, String> records = companion.consumeStrings().fromTopics(topic);

        Message<?> message = Message.of("hello").addMetadata(OutgoingCloudEventMetadata.builder()
                .withSource(URI.create("test://test"))
                .withType("type")
                .withId("some id")
                .withExtension("ext", 124)
                .withExtension("ext2", "bonjour")
                .build());

        Multi.createFrom().<Message<?>> item(message)
                .subscribe().withSubscriber((Subscriber) sink.getSink().build());

        await().until(() -> records.getRecords().size() == 1);

        ConsumerRecord<String, String> record = records.getRecords().get(0);
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
        ConsumerTask<String, String> records = companion.consumeStrings().fromTopics(topic);

        addBeans(Source.class, Processing.class, Sender.class);
        runApplication(getConfigToSendStructuredCloudEvents());

        await().until(() -> records.getRecords().size() >= 10);

        assertThat(records.getRecords()).allSatisfy(record -> {
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
        ConsumerTask<String, String> records = companion.consumeStrings().fromTopics(topic);

        addBeans(Source.class, Processing.class, Sender.class);
        runApplication(getConfigToSendBinaryCloudEvents());

        await().until(() -> records.getRecords().size() >= 10);

        assertThat(records.getRecords()).allSatisfy(record -> {
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
        ConsumerTask<String, String> records = companion.consumeStrings().fromTopics(topic);

        addBeans(Source.class, ConsumptionConsumerRebalanceListener.class);
        runApplication(getConfigToSendBinaryCloudEventsWithDefault());

        await().until(() -> records.getRecords().size() >= 10);

        assertThat(records.getRecords()).allSatisfy(record -> {
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
        ConsumerTask<String, String> records = companion.consumeStrings().fromTopics(topic);

        addBeans(Source.class, ConsumptionConsumerRebalanceListener.class);
        runApplication(getConfigToSendStructuredCloudEventsWithDefault());

        await().until(() -> records.getRecords().size() >= 10);

        assertThat(records.getRecords()).allSatisfy(record -> {
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

    private KafkaMapBasedConfig getConfigToSendStructuredCloudEvents() {
        KafkaMapBasedConfig config = kafkaConfig("mp.messaging.outgoing.kafka");
        config.put("value.serializer", StringSerializer.class.getName());
        config.put("cloud-events-mode", "structured");
        config.put("topic", topic);
        return config;
    }

    private KafkaMapBasedConfig getConfigToSendBinaryCloudEvents() {
        KafkaMapBasedConfig config = kafkaConfig("mp.messaging.outgoing.kafka");
        config.put("value.serializer", StringSerializer.class.getName());
        config.put("topic", topic);
        return config;
    }

    private KafkaMapBasedConfig getConfigToSendBinaryCloudEventsWithDefault() {
        KafkaMapBasedConfig config = kafkaConfig("mp.messaging.outgoing.source");
        config.put("value.serializer", StringSerializer.class.getName());
        config.put("topic", topic);
        config.put("cloud-events-type", "greetings");
        config.put("cloud-events-source", "source://me");
        config.put("cloud-events-subject", "test");
        return config;
    }

    private KafkaMapBasedConfig getConfigToSendStructuredCloudEventsWithDefault() {
        KafkaMapBasedConfig config = kafkaConfig("mp.messaging.outgoing.source");
        config.put("value.serializer", StringSerializer.class.getName());
        config.put("topic", topic);
        config.put("cloud-events-type", "greetings");
        config.put("cloud-events-source", "source://me");
        config.put("cloud-events-subject", "test");
        config.put("cloud-events-mode", "structured");
        return config;
    }

    private KafkaMapBasedConfig newCommonConfig() {
        String randomId = UUID.randomUUID().toString();
        return kafkaConfig()
                .put("bootstrap.servers", companion.getBootstrapServers())
                .put("group.id", randomId)
                .put("key.serializer", StringSerializer.class.getName())
                .put("enable.auto.commit", "false")
                .put("auto.offset.reset", "earliest")
                .put("tracing-enabled", false);
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
