package io.smallrye.reactive.messaging.kafka.ce;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.ce.CloudEventMetadata;
import io.smallrye.reactive.messaging.kafka.ConsumptionConsumerRebalanceListener;
import io.smallrye.reactive.messaging.kafka.CountKafkaCdiEvents;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaCloudEventMetadata;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;
import io.smallrye.reactive.messaging.kafka.base.UnsatisfiedInstance;
import io.smallrye.reactive.messaging.kafka.impl.KafkaSource;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.serialization.BufferDeserializer;
import io.vertx.kafka.client.serialization.JsonObjectDeserializer;
import io.vertx.kafka.client.serialization.JsonObjectSerializer;

public class KafkaSourceWithCloudEventsTest extends KafkaCompanionTestBase {

    KafkaSource<String, Integer> source;

    @BeforeAll
    public static void setup() {
        companion.registerSerde(JsonObject.class, Serdes.serdeFrom(new JsonObjectSerializer(), new JsonObjectDeserializer()));
    }

    @AfterEach
    public void stopping() {
        if (source != null) {
            source.closeQuietly();
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testReceivingStructuredCloudEventsWithStringDeserializer() {
        KafkaMapBasedConfig config = newCommonConfig();
        config.put("topic", topic);
        config.put("value.deserializer", StringDeserializer.class.getName());
        config.put("channel-name", topic);
        KafkaConnectorIncomingConfiguration ic = new KafkaConnectorIncomingConfiguration(config);
        source = new KafkaSource<>(vertx, UUID.randomUUID().toString(), ic,
                UnsatisfiedInstance.instance(), CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance(), -1);

        List<Message<?>> messages = new ArrayList<>();
        source.getStream().subscribe().with(messages::add);

        companion.produceStrings().fromRecords(
                new ProducerRecord<>(topic, null, null, null, new JsonObject()
                        .put("specversion", CloudEventMetadata.CE_VERSION_1_0)
                        .put("type", "type")
                        .put("id", "id")
                        .put("source", "test://test")
                        .put("subject", "foo")
                        .put("datacontenttype", "application/json")
                        .put("dataschema", "http://schema.io")
                        .put("time", "2020-07-23T09:12:34Z")
                        .put("data", new JsonObject().put("name", "neo")).encode(),
                        Collections.singletonList(
                                new RecordHeader("content-type",
                                        "application/cloudevents+json; charset=utf-8".getBytes()))));

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages.size() >= 1);

        Message<?> message = messages.get(0);
        IncomingKafkaCloudEventMetadata<String, JsonObject> metadata = message
                .getMetadata(IncomingKafkaCloudEventMetadata.class)
                .orElse(null);
        assertThat(metadata).isNotNull();
        assertThat(metadata.getSpecVersion()).isEqualTo(CloudEventMetadata.CE_VERSION_1_0);
        assertThat(metadata.getType()).isEqualTo("type");
        assertThat(metadata.getId()).isEqualTo("id");
        assertThat(metadata.getSource()).isEqualTo(URI.create("test://test"));
        assertThat(metadata.getSubject()).hasValue("foo");
        assertThat(metadata.getDataContentType()).hasValue("application/json");
        assertThat(metadata.getDataSchema()).hasValue(URI.create("http://schema.io"));
        assertThat(metadata.getTimeStamp()).isNotEmpty();
        assertThat(metadata.getData().getString("name")).isEqualTo("neo");

        // Extensions
        assertThat(metadata.getKey()).isNull();
        // Rule 3.1 - partitionkey attribute
        assertThat(metadata.<String> getExtension("partitionkey")).isEmpty();
        assertThat(metadata.getTopic()).isEqualTo(topic);

        assertThat(message.getPayload()).isInstanceOf(JsonObject.class);
        assertThat(((JsonObject) message.getPayload()).getString("name")).isEqualTo("neo");

    }

    @SuppressWarnings("unchecked")
    @Test
    public void testReceivingStructuredCloudEventsWithJsonObjectDeserializer() {
        KafkaMapBasedConfig config = newCommonConfig();
        config.put("topic", topic);
        config.put("value.deserializer", JsonObjectDeserializer.class.getName());
        config.put("channel-name", topic);
        KafkaConnectorIncomingConfiguration ic = new KafkaConnectorIncomingConfiguration(config);
        source = new KafkaSource<>(vertx, UUID.randomUUID().toString(), ic,
                UnsatisfiedInstance.instance(), CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance(), -1);

        List<Message<?>> messages = new ArrayList<>();
        source.getStream().subscribe().with(messages::add);

        companion.produce(String.class, JsonObject.class)
                .fromRecords(new ProducerRecord<>(topic, null, null, "key", new JsonObject()
                        .put("specversion", CloudEventMetadata.CE_VERSION_1_0)
                        .put("type", "type")
                        .put("id", "id")
                        .put("source", "test://test")
                        .put("subject", "foo")
                        .put("data", new JsonObject().put("name", "neo")),
                        Collections.singletonList(
                                new RecordHeader("content-type",
                                        "application/cloudevents+json; charset=utf-8".getBytes()))));

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages.size() >= 1);

        Message<?> message = messages.get(0);
        IncomingKafkaCloudEventMetadata<String, JsonObject> metadata = message
                .getMetadata(IncomingKafkaCloudEventMetadata.class).orElse(null);
        assertThat(metadata).isNotNull();
        assertThat(metadata.getSpecVersion()).isEqualTo(CloudEventMetadata.CE_VERSION_1_0);
        assertThat(metadata.getType()).isEqualTo("type");
        assertThat(metadata.getId()).isEqualTo("id");
        assertThat(metadata.getSource()).isEqualTo(URI.create("test://test"));
        assertThat(metadata.getSubject()).hasValue("foo");
        assertThat(metadata.getTimeStamp()).isEmpty();
        assertThat(metadata.getDataContentType()).isEmpty();
        assertThat(metadata.getDataSchema()).isEmpty();
        assertThat(metadata.getData().getString("name")).isEqualTo("neo");

        // Extensions
        assertThat(metadata.getKey()).isEqualTo("key");
        // Rule 3.1 - partitionkey attribute
        assertThat(metadata.<String> getExtension("partitionkey")).hasValue("key");
        assertThat(metadata.getTopic()).isEqualTo(topic);

        assertThat(message.getPayload()).isInstanceOf(JsonObject.class);
        assertThat(((JsonObject) message.getPayload()).getString("name")).isEqualTo("neo");

    }

    @SuppressWarnings("unchecked")
    @Test
    public void testReceivingStructuredCloudEventsWithByteArrayDeserializer() {
        KafkaMapBasedConfig config = newCommonConfig();
        config.put("topic", topic);
        config.put("value.deserializer", ByteArrayDeserializer.class.getName());
        config.put("channel-name", topic);
        KafkaConnectorIncomingConfiguration ic = new KafkaConnectorIncomingConfiguration(config);
        source = new KafkaSource<>(vertx, UUID.randomUUID().toString(), ic,
                UnsatisfiedInstance.instance(), CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance(), -1);

        List<Message<?>> messages = new ArrayList<>();
        source.getStream().subscribe().with(messages::add);

        companion.produce(String.class, JsonObject.class)
                .fromRecords(new ProducerRecord<>(topic, null, null, "key", new JsonObject()
                        .put("specversion", CloudEventMetadata.CE_VERSION_1_0)
                        .put("type", "type")
                        .put("id", "id")
                        .put("source", "test://test")
                        .put("subject", "foo")
                        .put("data", new JsonObject().put("name", "neo")),
                        Collections.singletonList(
                                new RecordHeader("content-type",
                                        "application/cloudevents+json; charset=utf-8".getBytes()))));

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages.size() >= 1);

        Message<?> message = messages.get(0);
        IncomingKafkaCloudEventMetadata<String, JsonObject> metadata = message
                .getMetadata(IncomingKafkaCloudEventMetadata.class).orElse(null);
        assertThat(metadata).isNotNull();
        assertThat(metadata.getSpecVersion()).isEqualTo(CloudEventMetadata.CE_VERSION_1_0);
        assertThat(metadata.getType()).isEqualTo("type");
        assertThat(metadata.getId()).isEqualTo("id");
        assertThat(metadata.getSource()).isEqualTo(URI.create("test://test"));
        assertThat(metadata.getSubject()).hasValue("foo");
        assertThat(metadata.getData().getString("name")).isEqualTo("neo");

        // Extensions
        assertThat(metadata.getKey()).isEqualTo("key");
        // Rule 3.1 - partitionkey attribute
        assertThat(metadata.<String> getExtension("partitionkey")).hasValue("key");
        assertThat(metadata.getTopic()).isEqualTo(topic);

        assertThat(message.getPayload()).isInstanceOf(JsonObject.class);
        assertThat(((JsonObject) message.getPayload()).getString("name")).isEqualTo("neo");

    }

    @Test
    public void testReceivingStructuredCloudEventsWithUnsupportedDeserializer() {
        KafkaMapBasedConfig config = newCommonConfig();
        config.put("topic", topic);
        // Unsupported on purpose
        config.put("value.deserializer", BufferDeserializer.class.getName());
        config.put("channel-name", topic);
        KafkaConnectorIncomingConfiguration ic = new KafkaConnectorIncomingConfiguration(config);
        source = new KafkaSource<>(vertx, UUID.randomUUID().toString(), ic,
                UnsatisfiedInstance.instance(), CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance(), -1);

        List<Message<?>> messages = new ArrayList<>();
        source.getStream().subscribe().with(messages::add);

        companion.produce(String.class, JsonObject.class)
                .fromRecords(new ProducerRecord<>(topic, null, null, "key",
                        new JsonObject()
                                .put("specversion", CloudEventMetadata.CE_VERSION_1_0)
                                .put("type", "type")
                                .put("id", "id")
                                .put("source", "test://test")
                                .put("data", new JsonObject().put("name", "neo")),
                        Collections.singletonList(
                                new RecordHeader("content-type",
                                        "application/cloudevents+json; charset=utf-8".getBytes()))));

        await()
                .pollDelay(Duration.ofSeconds(1))
                .atMost(2, TimeUnit.MINUTES).until(() -> messages.size() == 0);

        // Nothing has been received because the deserializer is not supported.
    }

    @Test
    public void testReceivingStructuredCloudEventsWithoutSource() {
        KafkaMapBasedConfig config = newCommonConfig();
        config.put("topic", topic);
        config.put("value.deserializer", JsonObjectDeserializer.class.getName());
        config.put("channel-name", topic);
        KafkaConnectorIncomingConfiguration ic = new KafkaConnectorIncomingConfiguration(config);
        source = new KafkaSource<>(vertx, UUID.randomUUID().toString(), ic,
                UnsatisfiedInstance.instance(), CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance(), -1);

        List<Message<?>> messages = new ArrayList<>();
        source.getStream().subscribe().with(messages::add);

        companion.produce(String.class, JsonObject.class)
                .fromRecords(new ProducerRecord<>(topic, null, null, "key",
                        new JsonObject()
                                .put("specversion", CloudEventMetadata.CE_VERSION_1_0)
                                .put("type", "type")
                                .put("id", "id")
                                .put("subject", "foo")
                                .put("data", new JsonObject().put("name", "neo")),
                        Collections.singletonList(
                                new RecordHeader("content-type",
                                        "application/cloudevents+json; charset=utf-8".getBytes()))));

        await()
                .pollDelay(Duration.ofSeconds(1))
                .atMost(2, TimeUnit.MINUTES).until(() -> messages.size() == 0);

        // Nothing has been received because the deserializer is not supported.

    }

    @SuppressWarnings("unchecked")
    @Test
    public void testReceivingBinaryCloudEvents() {
        KafkaMapBasedConfig config = newCommonConfig();
        config.put("topic", topic);
        config.put("value.deserializer", StringDeserializer.class.getName());
        config.put("channel-name", topic);
        KafkaConnectorIncomingConfiguration ic = new KafkaConnectorIncomingConfiguration(config);
        source = new KafkaSource<>(vertx, UUID.randomUUID().toString(), ic,
                UnsatisfiedInstance.instance(), CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance(), -1);

        List<Message<?>> messages = new ArrayList<>();
        source.getStream().subscribe().with(messages::add);

        List<Header> headers = new ArrayList<>();
        headers.add(new RecordHeader("ce_specversion", CloudEventMetadata.CE_VERSION_1_0.getBytes()));
        headers.add(new RecordHeader("ce_type", "type".getBytes()));
        headers.add(new RecordHeader("ce_source", "test://test".getBytes()));
        headers.add(new RecordHeader("ce_id", "id".getBytes()));
        headers.add(new RecordHeader("ce_time", "2020-07-23T07:59:04Z".getBytes()));
        headers.add(new RecordHeader("content-type", "text/plain".getBytes()));
        headers.add(new RecordHeader("ce_subject", "foo".getBytes()));
        headers.add(new RecordHeader("ce_dataschema", "http://schema.io".getBytes()));
        headers.add(new RecordHeader("ce_ext", "bar".getBytes()));
        headers.add(new RecordHeader("some-header", "baz".getBytes()));
        companion.produceStrings()
                .fromRecords(new ProducerRecord<>(topic, null, null, "key", "Hello World", headers));

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages.size() >= 1);

        Message<?> message = messages.get(0);
        IncomingKafkaCloudEventMetadata<String, String> metadata = message.getMetadata(IncomingKafkaCloudEventMetadata.class)
                .orElse(null);
        assertThat(metadata).isNotNull();
        assertThat(metadata.getSpecVersion()).isEqualTo(CloudEventMetadata.CE_VERSION_1_0);
        assertThat(metadata.getType()).isEqualTo("type");
        assertThat(metadata.getId()).isEqualTo("id");
        assertThat(metadata.getSource()).isEqualTo(URI.create("test://test"));

        assertThat(metadata.getSubject()).hasValue("foo");
        assertThat(metadata.getDataSchema()).hasValue(URI.create("http://schema.io"));
        assertThat(metadata.getTimeStamp()).isNotEmpty();

        assertThat(metadata.getData()).isEqualTo("Hello World");
        // Rule 3.2.1 - the content-type must be mapped to the datacontenttype attribute
        assertThat(metadata.getDataContentType()).hasValue("text/plain");
        // Rule 3.2.3
        assertThat(metadata.getExtension("ext")).hasValue("bar");
        assertThat(metadata.getExtension("some-header")).isEmpty();

        // Extensions
        assertThat(metadata.getKey()).isEqualTo("key");
        // Rule 3.1 - partitionkey attribute
        assertThat(metadata.<String> getExtension("partitionkey")).hasValue("key");
        assertThat(metadata.getTopic()).isEqualTo(topic);

        assertThat(message.getPayload()).isInstanceOf(String.class).isEqualTo("Hello World");

    }

    @SuppressWarnings("unchecked")
    @Test
    public void testReceivingBinaryCloudEventsWithoutKey() {
        KafkaMapBasedConfig config = newCommonConfig();
        config.put("topic", topic);
        config.put("value.deserializer", StringDeserializer.class.getName());
        config.put("channel-name", topic);
        KafkaConnectorIncomingConfiguration ic = new KafkaConnectorIncomingConfiguration(config);
        source = new KafkaSource<>(vertx, UUID.randomUUID().toString(), ic,
                UnsatisfiedInstance.instance(), CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance(), -1);

        List<Message<?>> messages = new ArrayList<>();
        source.getStream().subscribe().with(messages::add);

        List<Header> headers = new ArrayList<>();
        headers.add(new RecordHeader("ce_specversion", CloudEventMetadata.CE_VERSION_1_0.getBytes()));
        headers.add(new RecordHeader("ce_type", "type".getBytes()));
        headers.add(new RecordHeader("ce_source", "test://test".getBytes()));
        headers.add(new RecordHeader("ce_id", "id".getBytes()));
        headers.add(new RecordHeader("ce_time", "2020-07-23T07:59:04Z".getBytes()));
        headers.add(new RecordHeader("content-type", "text/plain".getBytes()));
        headers.add(new RecordHeader("ce_subject", "foo".getBytes()));
        headers.add(new RecordHeader("ce_dataschema", "http://schema.io".getBytes()));
        headers.add(new RecordHeader("ce_ext", "bar".getBytes()));
        headers.add(new RecordHeader("some-header", "baz".getBytes()));
        companion.produceStrings()
                .fromRecords(new ProducerRecord<>(topic, null, null, null, "Hello World", headers));

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages.size() >= 1);

        Message<?> message = messages.get(0);
        IncomingKafkaCloudEventMetadata<String, String> metadata = message.getMetadata(IncomingKafkaCloudEventMetadata.class)
                .orElse(null);
        assertThat(metadata).isNotNull();
        assertThat(metadata.getSpecVersion()).isEqualTo(CloudEventMetadata.CE_VERSION_1_0);
        assertThat(metadata.getType()).isEqualTo("type");
        assertThat(metadata.getId()).isEqualTo("id");
        assertThat(metadata.getSource()).isEqualTo(URI.create("test://test"));

        assertThat(metadata.getSubject()).hasValue("foo");
        assertThat(metadata.getDataSchema()).hasValue(URI.create("http://schema.io"));
        assertThat(metadata.getTimeStamp()).isNotEmpty();

        assertThat(metadata.getData()).isEqualTo("Hello World");
        // Rule 3.2.1 - the content-type must be mapped to the datacontenttype attribute
        assertThat(metadata.getDataContentType()).hasValue("text/plain");
        // Rule 3.2.3
        assertThat(metadata.getExtension("ext")).hasValue("bar");
        assertThat(metadata.getExtension("some-header")).isEmpty();

        // Extensions
        assertThat(metadata.getKey()).isNull();
        // Rule 3.1 - partitionkey attribute
        assertThat(metadata.<String> getExtension("partitionkey")).isEmpty();
        assertThat(metadata.getTopic()).isEqualTo(topic);

        assertThat(message.getPayload()).isInstanceOf(String.class).isEqualTo("Hello World");

    }

    @SuppressWarnings("unchecked")
    @Test
    public void testReceivingStructuredCloudEventsWithoutMatchingContentTypeIsNotReadACloudEvent() {
        KafkaMapBasedConfig config = newCommonConfig();
        config.put("topic", topic);
        config.put("value.deserializer", StringDeserializer.class.getName());
        config.put("channel-name", topic);
        KafkaConnectorIncomingConfiguration ic = new KafkaConnectorIncomingConfiguration(config);
        source = new KafkaSource<>(vertx, UUID.randomUUID().toString(), ic,
                UnsatisfiedInstance.instance(), CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance(), -1);

        List<Message<?>> messages = new ArrayList<>();
        source.getStream().subscribe().with(messages::add);

        companion.produceStrings()
                .fromRecords(new ProducerRecord<>(topic, null, null, "key",
                        new JsonObject()
                                .put("specversion", CloudEventMetadata.CE_VERSION_1_0)
                                .put("type", "type")
                                .put("id", "id")
                                .put("source", "test://test")
                                .put("data", new JsonObject().put("name", "neo")).encode(),
                        Collections.singletonList(
                                new RecordHeader("content-type", "application/json; charset=utf-8".getBytes()))));

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages.size() >= 1);

        Message<?> message = messages.get(0);
        IncomingKafkaCloudEventMetadata<String, JsonObject> metadata = message
                .getMetadata(IncomingKafkaCloudEventMetadata.class)
                .orElse(null);
        assertThat(metadata).isNull();
        CloudEventMetadata<JsonObject> metadata2 = message
                .getMetadata(CloudEventMetadata.class)
                .orElse(null);
        assertThat(metadata2).isNull();
        assertThat(message.getPayload()).isInstanceOf(String.class);
        JsonObject json = new JsonObject(message.getPayload().toString());
        assertThat(json.getString("id")).isEqualTo("id");

    }

    @SuppressWarnings("unchecked")
    @Test
    public void testWithBeanReceivingBinaryAndStructuredCloudEvents() {
        ConsumptionBean bean = run(getConfig(topic));

        List<KafkaRecord<String, String>> list = bean.getKafkaRecords();
        assertThat(list).isEmpty();

        // Send a binary cloud event
        List<Header> headers = new ArrayList<>();
        headers.add(new RecordHeader("ce_specversion", CloudEventMetadata.CE_VERSION_1_0.getBytes()));
        headers.add(new RecordHeader("ce_type", "type".getBytes()));
        headers.add(new RecordHeader("ce_source", "test://test".getBytes()));
        headers.add(new RecordHeader("ce_id", "id".getBytes()));
        headers.add(new RecordHeader("content-type", "text/plain".getBytes()));
        headers.add(new RecordHeader("ce_subject", "foo".getBytes()));
        companion.produceStrings()
                .fromRecords(new ProducerRecord<>(topic, null, null, "binary", "Hello Binary 1", headers));

        await().atMost(10, TimeUnit.SECONDS).until(() -> list.size() >= 1);
        KafkaRecord<String, String> record = list.get(0);
        assertThat(record.getTopic()).isEqualTo(topic);
        IncomingKafkaCloudEventMetadata<String, String> metadata = record
                .getMetadata(IncomingKafkaCloudEventMetadata.class).orElse(null);
        assertThat(metadata).isNotNull();
        assertThat(metadata.getTopic()).isEqualTo(topic);
        assertThat(metadata.getKey()).isEqualTo("binary");
        assertThat(metadata.getId()).isEqualTo("id");
        assertThat(metadata.getSubject()).hasValue("foo");
        assertThat(metadata.getData()).isEqualTo("Hello Binary 1");
        assertThat(record.getPayload()).isEqualTo("Hello Binary 1");

        // send a structured event
        companion.produceStrings()
                .fromRecords(new ProducerRecord<>(topic, null, null, "structured",
                        new JsonObject()
                                .put("specversion", CloudEventMetadata.CE_VERSION_1_0)
                                .put("type", "type")
                                .put("id", "id")
                                .put("source", "test://test")
                                .put("subject", "bar")
                                .put("datacontenttype", "application/json")
                                .put("dataschema", "http://schema.io")
                                .put("time", "2020-07-23T09:12:34Z")
                                .put("data", "Hello Structured 1").encode(),
                        Collections.singletonList(
                                new RecordHeader("content-type",
                                        "application/cloudevents+json; charset=utf-8".getBytes()))));

        await().atMost(10, TimeUnit.SECONDS).until(() -> list.size() >= 2);
        record = list.get(1);
        assertThat(record.getTopic()).isEqualTo(topic);
        metadata = record
                .getMetadata(IncomingKafkaCloudEventMetadata.class).orElse(null);
        assertThat(metadata).isNotNull();
        assertThat(metadata.getTopic()).isEqualTo(topic);
        assertThat(metadata.getKey()).isEqualTo("structured");
        assertThat(metadata.getId()).isEqualTo("id");
        assertThat(metadata.getSubject()).hasValue("bar");
        assertThat(metadata.getData()).isEqualTo("Hello Structured 1");
        assertThat(record.getPayload()).contains("Hello Structured 1");

        // Send a last binary cloud event
        List<Header> headers2 = new ArrayList<>();
        headers2.add(new RecordHeader("ce_specversion", CloudEventMetadata.CE_VERSION_1_0.getBytes()));
        headers2.add(new RecordHeader("ce_type", "type".getBytes()));
        headers2.add(new RecordHeader("ce_source", "test://test".getBytes()));
        headers2.add(new RecordHeader("ce_id", "id".getBytes()));
        headers2.add(new RecordHeader("content-type", "text/plain".getBytes()));
        headers2.add(new RecordHeader("ce_subject", "foo".getBytes()));
        companion.produceStrings()
                .fromRecords(new ProducerRecord<>(topic, null, null, "binary", "Hello Binary 2", headers2));

        await().atMost(10, TimeUnit.SECONDS).until(() -> list.size() >= 3);
        record = list.get(2);
        assertThat(record.getTopic()).isEqualTo(topic);
        metadata = record
                .getMetadata(IncomingKafkaCloudEventMetadata.class).orElse(null);
        assertThat(metadata).isNotNull();
        assertThat(metadata.getTopic()).isEqualTo(topic);
        assertThat(metadata.getKey()).isEqualTo("binary");
        assertThat(metadata.getId()).isEqualTo("id");
        assertThat(metadata.getSubject()).hasValue("foo");
        assertThat(metadata.getData()).isEqualTo("Hello Binary 2");
        assertThat(record.getPayload()).isEqualTo("Hello Binary 2");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testReceivingBinaryCloudEventsWithSupportDisabled() {
        KafkaMapBasedConfig config = newCommonConfig();
        config.put("topic", topic);
        config.put("value.deserializer", StringDeserializer.class.getName());
        config.put("channel-name", topic);
        config.put("cloud-events", false);
        KafkaConnectorIncomingConfiguration ic = new KafkaConnectorIncomingConfiguration(config);
        source = new KafkaSource<>(vertx, UUID.randomUUID().toString(), ic,
                UnsatisfiedInstance.instance(), CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance(), -1);

        List<Message<?>> messages = new ArrayList<>();
        source.getStream().subscribe().with(messages::add);

        List<Header> headers = new ArrayList<>();
        headers.add(new RecordHeader("ce_specversion", CloudEventMetadata.CE_VERSION_1_0.getBytes()));
        headers.add(new RecordHeader("ce_type", "type".getBytes()));
        headers.add(new RecordHeader("ce_source", "test://test".getBytes()));
        headers.add(new RecordHeader("ce_id", "id".getBytes()));
        headers.add(new RecordHeader("ce_time", "2020-07-23T07:59:04Z".getBytes()));
        headers.add(new RecordHeader("content-type", "text/plain".getBytes()));
        headers.add(new RecordHeader("ce_subject", "foo".getBytes()));
        headers.add(new RecordHeader("ce_dataschema", "http://schema.io".getBytes()));
        headers.add(new RecordHeader("ce_ext", "bar".getBytes()));
        headers.add(new RecordHeader("some-header", "baz".getBytes()));
        companion.produceStrings()
                .fromRecords(new ProducerRecord<>(topic, null, null, "key", "Hello World", headers));

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages.size() >= 1);

        Message<?> message = messages.get(0);
        IncomingKafkaCloudEventMetadata<String, String> metadata = message.getMetadata(IncomingKafkaCloudEventMetadata.class)
                .orElse(null);
        assertThat(metadata).isNull();
        assertThat(message.getPayload()).isInstanceOf(String.class).isEqualTo("Hello World");

    }

    @SuppressWarnings("unchecked")
    @Test
    public void testReceivingStructuredCloudEventsWithSupportDisabled() {
        KafkaMapBasedConfig config = newCommonConfig();
        config.put("topic", topic);
        config.put("value.deserializer", JsonObjectDeserializer.class.getName());
        config.put("channel-name", topic);
        config.put("cloud-events", false);
        KafkaConnectorIncomingConfiguration ic = new KafkaConnectorIncomingConfiguration(config);
        source = new KafkaSource<>(vertx, UUID.randomUUID().toString(), ic,
                UnsatisfiedInstance.instance(), CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance(), -1);

        List<Message<?>> messages = new ArrayList<>();
        source.getStream().subscribe().with(messages::add);

        companion.produceStrings()
                .fromRecords(new ProducerRecord<>(topic, null, null, null,
                        new JsonObject()
                                .put("specversion", CloudEventMetadata.CE_VERSION_1_0)
                                .put("type", "type")
                                .put("id", "id")
                                .put("source", "test://test")
                                .put("subject", "foo")
                                .put("datacontenttype", "application/json")
                                .put("dataschema", "http://schema.io")
                                .put("time", "2020-07-23T09:12:34Z")
                                .put("data", new JsonObject().put("name", "neo")).encode(),
                        Collections.singletonList(
                                new RecordHeader("content-type",
                                        "application/cloudevents+json; charset=utf-8".getBytes()))));

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages.size() >= 1);

        Message<?> message = messages.get(0);
        IncomingKafkaCloudEventMetadata<String, JsonObject> metadata = message
                .getMetadata(IncomingKafkaCloudEventMetadata.class)
                .orElse(null);
        assertThat(metadata).isNull();
        assertThat(message.getPayload()).isInstanceOf(JsonObject.class);
        assertThat(((JsonObject) message.getPayload()).getJsonObject("data").getString("name")).isEqualTo("neo");

    }

    @SuppressWarnings("unchecked")
    @Test
    public void testReceivingStructuredCloudEventsNoData() {
        KafkaMapBasedConfig config = newCommonConfig();
        config.put("topic", topic);
        config.put("value.deserializer", StringDeserializer.class.getName());
        config.put("channel-name", topic);
        KafkaConnectorIncomingConfiguration ic = new KafkaConnectorIncomingConfiguration(config);
        source = new KafkaSource<>(vertx, UUID.randomUUID().toString(), ic,
                UnsatisfiedInstance.instance(), CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance(), -1);

        List<Message<?>> messages = new ArrayList<>();
        source.getStream().subscribe().with(messages::add);

        companion.produceStrings()
                .fromRecords(new ProducerRecord<>(topic, null, null, null,
                        new JsonObject()
                                .put("specversion", CloudEventMetadata.CE_VERSION_1_0)
                                .put("type", "type")
                                .put("id", "id")
                                .put("source", "test://test").encode(),
                        Collections.singletonList(
                                new RecordHeader("content-type",
                                        "application/cloudevents+json; charset=utf-8".getBytes()))));

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages.size() >= 1);

        Message<?> message = messages.get(0);
        IncomingKafkaCloudEventMetadata<String, JsonObject> metadata = message
                .getMetadata(IncomingKafkaCloudEventMetadata.class)
                .orElse(null);
        assertThat(metadata).isNotNull();
        assertThat(metadata.getSpecVersion()).isEqualTo(CloudEventMetadata.CE_VERSION_1_0);
        assertThat(metadata.getType()).isEqualTo("type");
        assertThat(metadata.getId()).isEqualTo("id");
        assertThat(metadata.getSource()).isEqualTo(URI.create("test://test"));
        assertThat(metadata.getData()).isNull();

        assertThat(message.getPayload()).isNull();

    }

    private ConsumptionBean run(KafkaMapBasedConfig config) {
        addBeans(ConsumptionBean.class, ConsumptionConsumerRebalanceListener.class);
        runApplication(config);
        return get(ConsumptionBean.class);
    }

    private KafkaMapBasedConfig getConfig(String topic) {
        KafkaMapBasedConfig config = kafkaConfig("mp.messaging.incoming.data");
        config.put("value.deserializer", StringDeserializer.class.getName());
        config.put("enable.auto.commit", "false");
        config.put("auto.offset.reset", "earliest");
        config.put("topic", topic);
        return config;
    }

    private KafkaMapBasedConfig newCommonConfig() {
        String randomId = UUID.randomUUID().toString();
        return kafkaConfig()
                .put("bootstrap.servers", companion.getBootstrapServers())
                .put("group.id", randomId)
                .put("key.deserializer", StringDeserializer.class.getName())
                .put("graceful-shutdown", false)
                .put("enable.auto.commit", "false")
                .put("auto.offset.reset", "earliest")
                .put("tracing-enabled", false);
    }

    @ApplicationScoped
    public static class ConsumptionBean {

        private final List<KafkaRecord<String, String>> kafka = new CopyOnWriteArrayList<>();

        @Incoming("data")
        public CompletionStage<Void> process(KafkaRecord<String, String> input) {
            kafka.add(input);
            return input.ack();
        }

        public List<KafkaRecord<String, String>> getKafkaRecords() {
            return kafka;
        }
    }

}
