package io.smallrye.reactive.messaging.amqp.ce;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.awaitility.Awaitility.await;

import java.net.URI;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.reactive.messaging.amqp.*;
import io.smallrye.reactive.messaging.ce.CloudEventMetadata;
import io.smallrye.reactive.messaging.ce.IncomingCloudEventMetadata;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.amqp.AmqpMessage;
import io.vertx.mutiny.core.buffer.Buffer;

@SuppressWarnings("unchecked")
public class CloudEventConsumptionTest extends AmqpBrokerTestBase {

    private WeldContainer container;
    private final Weld weld = new Weld();

    @AfterEach
    public void cleanup() {
        if (container != null) {
            container.close();
        }
        // Release the config objects
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
    }

    @Test
    public void testReceivingStructuredCloudEventsWithJsonObjectPayload() {
        String address = UUID.randomUUID().toString();

        new MapBasedConfig()
                .with("mp.messaging.incoming.source.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.source.address", address)
                .with("mp.messaging.incoming.source.host", host)
                .with("mp.messaging.incoming.source.port", port)
                .with("mp.messaging.incoming.source.tracing-enabled", false)
                .with("amqp-username", username)
                .with("amqp-password", password)
                .write();

        weld.addBeanClass(MyConsumptionBean.class);
        container = weld.initialize();
        await().until(() -> isAmqpConnectorReady(container));
        MyConsumptionBean bean = container.getBeanManager().createInstance().select(MyConsumptionBean.class).get();

        usage.produce(address, 1, () -> {
            JsonObject json = new JsonObject()
                    .put("specversion", CloudEventMetadata.CE_VERSION_1_0)
                    .put("type", "type")
                    .put("id", "id")
                    .put("source", "test://test")
                    .put("subject", "foo")
                    .put("datacontenttype", "application/json")
                    .put("dataschema", "http://schema.io")
                    .put("time", "2020-07-23T09:12:34Z")
                    .put("data", new JsonObject().put("name", "neo"));

            return AmqpMessage.create()
                    .withJsonObjectAsBody(json)
                    .contentType(AmqpCloudEventHelper.STRUCTURED_CONTENT_TYPE).build();
        });

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.getList().size() >= 1);
        Message<JsonObject> message = bean.getList().get(0);
        IncomingCloudEventMetadata<JsonObject> metadata = message
                .getMetadata(IncomingCloudEventMetadata.class)
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

        assertThat(message.getPayload()).isInstanceOf(JsonObject.class);
        assertThat(message.getPayload().getString("name")).isEqualTo("neo");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testReceivingStructuredCloudEventsWithStringPayload() {
        String address = UUID.randomUUID().toString();

        new MapBasedConfig()
                .with("mp.messaging.incoming.source.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.source.address", address)
                .with("mp.messaging.incoming.source.host", host)
                .with("mp.messaging.incoming.source.port", port)
                .with("mp.messaging.incoming.source.tracing-enabled", false)
                .with("amqp-username", username)
                .with("amqp-password", password)
                .write();

        weld.addBeanClass(MyConsumptionBean.class);
        container = weld.initialize();
        await().until(() -> isAmqpConnectorReady(container));
        MyConsumptionBean bean = container.getBeanManager().createInstance().select(MyConsumptionBean.class).get();

        usage.produce(address, 1, () -> {
            JsonObject json = new JsonObject()
                    .put("specversion", CloudEventMetadata.CE_VERSION_1_0)
                    .put("type", "type")
                    .put("id", "id")
                    .put("source", "test://test")
                    .put("subject", "foo")
                    .put("datacontenttype", "application/json")
                    .put("dataschema", "http://schema.io")
                    .put("time", "2020-07-23T09:12:34Z")
                    .put("data", new JsonObject().put("name", "neo"));

            return AmqpMessage.create()
                    .withBody(json.encode())
                    .contentType(AmqpCloudEventHelper.STRUCTURED_CONTENT_TYPE).build();
        });

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.getList().size() >= 1);
        Message<JsonObject> message = bean.getList().get(0);
        IncomingCloudEventMetadata<JsonObject> metadata = message
                .getMetadata(IncomingCloudEventMetadata.class)
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

        assertThat(message.getPayload()).isInstanceOf(JsonObject.class);
        assertThat(message.getPayload().getString("name")).isEqualTo("neo");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testReceivingStructuredCloudEventsWithBinaryPayload() {
        String address = UUID.randomUUID().toString();

        new MapBasedConfig()
                .with("mp.messaging.incoming.source.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.source.address", address)
                .with("mp.messaging.incoming.source.host", host)
                .with("mp.messaging.incoming.source.port", port)
                .with("mp.messaging.incoming.source.tracing-enabled", false)
                .with("amqp-username", username)
                .with("amqp-password", password)
                .write();

        weld.addBeanClass(MyConsumptionBean.class);
        container = weld.initialize();
        await().until(() -> isAmqpConnectorReady(container));
        MyConsumptionBean bean = container.getBeanManager().createInstance().select(MyConsumptionBean.class).get();

        usage.produce(address, 1, () -> {
            JsonObject json = new JsonObject()
                    .put("specversion", CloudEventMetadata.CE_VERSION_1_0)
                    .put("type", "type")
                    .put("id", "id")
                    .put("source", "test://test")
                    .put("subject", "foo")
                    .put("datacontenttype", "application/json")
                    .put("dataschema", "http://schema.io")
                    .put("time", "2020-07-23T09:12:34Z")
                    .put("data", new JsonObject().put("name", "neo"));

            return AmqpMessage.create()
                    .withBufferAsBody(Buffer.buffer(json.toBuffer().getBytes()))
                    .contentType(AmqpCloudEventHelper.STRUCTURED_CONTENT_TYPE).build();
        });

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.getList().size() >= 1);
        Message<JsonObject> message = bean.getList().get(0);
        IncomingCloudEventMetadata<JsonObject> metadata = message
                .getMetadata(IncomingCloudEventMetadata.class)
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

        assertThat(message.getPayload()).isInstanceOf(JsonObject.class);
        assertThat(message.getPayload().getString("name")).isEqualTo("neo");
    }

    @Test
    public void testReceivingStructuredCloudEventsWithoutSource() {
        String address = UUID.randomUUID().toString();

        new MapBasedConfig()
                .with("mp.messaging.incoming.source.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.source.address", address)
                .with("mp.messaging.incoming.source.host", host)
                .with("mp.messaging.incoming.source.port", port)
                .with("mp.messaging.incoming.source.tracing-enabled", false)
                .with("amqp-username", username)
                .with("amqp-password", password)
                .write();

        weld.addBeanClass(MyConsumptionBean.class);
        container = weld.initialize();
        await().until(() -> isAmqpConnectorReady(container));
        MyConsumptionBean bean = container.getBeanManager().createInstance().select(MyConsumptionBean.class).get();

        usage.produce(address, 1, () -> {
            JsonObject json = new JsonObject()
                    .put("specversion", CloudEventMetadata.CE_VERSION_1_0)
                    .put("type", "type")
                    .put("id", "id")
                    .put("subject", "foo")
                    .put("data", new JsonObject().put("name", "neo"));

            return AmqpMessage.create()
                    .withJsonObjectAsBody(json)
                    .contentType(AmqpCloudEventHelper.STRUCTURED_CONTENT_TYPE).build();
        });

        await()
                .pollDelay(Duration.ofSeconds(1))
                .atMost(2, TimeUnit.MINUTES).until(() -> bean.getList().size() == 0);

        // Nothing has been received because the deserializer is not supported.
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testReceivingBinaryCloudEventsUsingJsonPayload() {
        String address = UUID.randomUUID().toString();

        new MapBasedConfig()
                .with("mp.messaging.incoming.source.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.source.address", address)
                .with("mp.messaging.incoming.source.host", host)
                .with("mp.messaging.incoming.source.port", port)
                .with("mp.messaging.incoming.source.tracing-enabled", false)
                .with("amqp-username", username)
                .with("amqp-password", password)
                .write();

        weld.addBeanClass(MyConsumptionBean.class);
        container = weld.initialize();
        await().until(() -> isAmqpConnectorReady(container));
        MyConsumptionBean bean = container.getBeanManager().createInstance().select(MyConsumptionBean.class).get();

        usage.produce(address, 1, () -> {
            JsonObject json = new JsonObject()
                    .put(AmqpCloudEventHelper.AMQP_HEADER_FOR_SPEC_VERSION, CloudEventMetadata.CE_VERSION_1_0)
                    .put(AmqpCloudEventHelper.AMQP_HEADER_FOR_TYPE, "type")
                    .put(AmqpCloudEventHelper.AMQP_HEADER_FOR_ID, "id")
                    .put(AmqpCloudEventHelper.AMQP_HEADER_FOR_SOURCE, "test://test")
                    .put(AmqpCloudEventHelper.AMQP_HEADER_FOR_SUBJECT, "foo")
                    .put(AmqpCloudEventHelper.AMQP_HEADER_FOR_SCHEMA, "http://schema.io")
                    .put(AmqpCloudEventHelper.AMQP_HEADER_FOR_TIME, "2020-07-23T09:12:34Z");
            JsonObject payload = new JsonObject();
            payload.put("name", "neo");

            return AmqpMessage.create()
                    .applicationProperties(json)
                    .withJsonObjectAsBody(payload)
                    .build();
        });

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.getList().size() >= 1);
        Message<JsonObject> message = bean.getList().get(0);
        IncomingCloudEventMetadata<JsonObject> metadata = message
                .getMetadata(IncomingCloudEventMetadata.class)
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

        assertThat(message.getPayload()).isInstanceOf(JsonObject.class);
        assertThat(message.getPayload().getString("name")).isEqualTo("neo");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testReceivingBinaryCloudEventsUsingStringPayload() {
        String address = UUID.randomUUID().toString();

        new MapBasedConfig()
                .with("mp.messaging.incoming.source.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.source.address", address)
                .with("mp.messaging.incoming.source.host", host)
                .with("mp.messaging.incoming.source.port", port)
                .with("mp.messaging.incoming.source.tracing-enabled", false)
                .with("amqp-username", username)
                .with("amqp-password", password)
                .write();

        weld.addBeanClass(MyStringConsumptionBean.class);
        container = weld.initialize();
        await().until(() -> isAmqpConnectorReady(container));

        MyStringConsumptionBean bean = container.getBeanManager().createInstance().select(MyStringConsumptionBean.class).get();

        usage.produce(address, 1, () -> {
            JsonObject json = new JsonObject()
                    .put(AmqpCloudEventHelper.AMQP_HEADER_FOR_SPEC_VERSION, CloudEventMetadata.CE_VERSION_1_0)
                    .put(AmqpCloudEventHelper.AMQP_HEADER_FOR_TYPE, "type")
                    .put(AmqpCloudEventHelper.AMQP_HEADER_FOR_ID, "id")
                    .put(AmqpCloudEventHelper.AMQP_HEADER_FOR_SOURCE, "test://test")
                    .put(AmqpCloudEventHelper.AMQP_HEADER_FOR_SUBJECT, "foo")
                    .put(AmqpCloudEventHelper.AMQP_HEADER_FOR_SCHEMA, "http://schema.io")
                    .put(AmqpCloudEventHelper.AMQP_HEADER_FOR_TIME, "2020-07-23T09:12:34Z")
                    .put(AmqpCloudEventHelper.CE_HEADER_PREFIX + "ext", "hello");

            return AmqpMessage.create()
                    .applicationProperties(json)
                    .contentType("text/plain")
                    .withBody("Hello World")
                    .build();
        });

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.getList().size() >= 1);
        Message<String> message = bean.getList().get(0);
        IncomingCloudEventMetadata<String> metadata = message
                .getMetadata(IncomingCloudEventMetadata.class)
                .orElse(null);
        assertThat(metadata).isNotNull();
        assertThat(metadata.getSpecVersion()).isEqualTo(CloudEventMetadata.CE_VERSION_1_0);
        assertThat(metadata.getType()).isEqualTo("type");
        assertThat(metadata.getId()).isEqualTo("id");
        assertThat(metadata.getSource()).isEqualTo(URI.create("test://test"));
        assertThat(metadata.getSubject()).hasValue("foo");
        assertThat(metadata.getDataContentType()).hasValue("text/plain");
        assertThat(metadata.getDataSchema()).hasValue(URI.create("http://schema.io"));
        assertThat(metadata.getTimeStamp()).isNotEmpty();

        assertThat(metadata.getExtensions()).containsEntry("ext", "hello");

        assertThat(metadata.getData()).isEqualTo("Hello World");

        assertThat(message.getPayload()).isInstanceOf(String.class);
        assertThat(message.getPayload()).isEqualTo("Hello World");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testReceivingStructuredCloudEventsWithoutMatchingContentTypeIsNotReadACloudEvent() {
        String address = UUID.randomUUID().toString();

        new MapBasedConfig()
                .with("mp.messaging.incoming.source.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.source.address", address)
                .with("mp.messaging.incoming.source.host", host)
                .with("mp.messaging.incoming.source.port", port)
                .with("mp.messaging.incoming.source.tracing-enabled", false)
                .with("amqp-username", username)
                .with("amqp-password", password)
                .write();

        weld.addBeanClass(MyConsumptionBean.class);
        container = weld.initialize();
        await().until(() -> isAmqpConnectorReady(container));
        MyConsumptionBean bean = container.getBeanManager().createInstance().select(MyConsumptionBean.class).get();

        usage.produce(address, 1, () -> {
            JsonObject json = new JsonObject()
                    .put("specversion", CloudEventMetadata.CE_VERSION_1_0)
                    .put("type", "type")
                    .put("id", "id")
                    .put("source", "test://test")
                    .put("data", new JsonObject().put("name", "neo"));

            return AmqpMessage.create()
                    .withJsonObjectAsBody(json) // Set the content type to application/json
                    .build();
        });

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.getList().size() >= 1);
        Message<JsonObject> message = bean.getList().get(0);
        IncomingCloudEventMetadata<JsonObject> metadata = message
                .getMetadata(IncomingCloudEventMetadata.class)
                .orElse(null);
        assertThat(metadata).isNull();
        assertThat(message.getPayload().getString("id")).isEqualTo("id");
    }

    @Test
    public void testWithBeanReceivingBinaryAndStructuredCloudEvents() {
        String address = UUID.randomUUID().toString();

        new MapBasedConfig()
                .with("mp.messaging.incoming.source.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.source.address", address)
                .with("mp.messaging.incoming.source.host", host)
                .with("mp.messaging.incoming.source.port", port)
                .with("mp.messaging.incoming.source.tracing-enabled", false)
                .with("amqp-username", username)
                .with("amqp-password", password)
                .write();

        weld.addBeanClass(MyConsumptionBean.class);
        container = weld.initialize();
        await().until(() -> isAmqpConnectorReady(container));
        MyConsumptionBean bean = container.getBeanManager().createInstance().select(MyConsumptionBean.class).get();

        usage.produce(address, 1, () -> {
            JsonObject json = new JsonObject()
                    .put(AmqpCloudEventHelper.AMQP_HEADER_FOR_SPEC_VERSION, CloudEventMetadata.CE_VERSION_1_0)
                    .put(AmqpCloudEventHelper.AMQP_HEADER_FOR_TYPE, "type")
                    .put(AmqpCloudEventHelper.AMQP_HEADER_FOR_ID, "id")
                    .put(AmqpCloudEventHelper.AMQP_HEADER_FOR_SOURCE, "test://test")
                    .put(AmqpCloudEventHelper.AMQP_HEADER_FOR_SUBJECT, "foo")
                    .put(AmqpCloudEventHelper.AMQP_HEADER_FOR_SCHEMA, "http://schema.io")
                    .put(AmqpCloudEventHelper.AMQP_HEADER_FOR_TIME, "2020-07-23T09:12:34Z");
            JsonObject payload = new JsonObject();
            payload.put("name", "neo");

            return AmqpMessage.create()
                    .applicationProperties(json)
                    .withJsonObjectAsBody(payload)
                    .build();
        });

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.getList().size() == 1);
        Message<JsonObject> message = bean.getList().get(0);
        IncomingCloudEventMetadata<JsonObject> metadata = message
                .getMetadata(IncomingCloudEventMetadata.class)
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

        assertThat(message.getPayload()).isInstanceOf(JsonObject.class);
        assertThat(message.getPayload().getString("name")).isEqualTo("neo");

        usage.produce(address, 1, () -> {
            JsonObject json = new JsonObject()
                    .put("specversion", CloudEventMetadata.CE_VERSION_1_0)
                    .put("type", "type")
                    .put("id", "id")
                    .put("source", "test://test")
                    .put("subject", "foo")
                    .put("datacontenttype", "application/json")
                    .put("dataschema", "http://schema.io")
                    .put("time", "2020-07-23T09:12:34Z")
                    .put("data", new JsonObject().put("name", "neo-2"));

            return AmqpMessage.create()
                    .withBufferAsBody(Buffer.buffer(json.toBuffer().getBytes()))
                    .contentType(AmqpCloudEventHelper.STRUCTURED_CONTENT_TYPE).build();
        });

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.getList().size() == 2);
        message = bean.getList().get(1);
        metadata = message
                .getMetadata(IncomingCloudEventMetadata.class)
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
        assertThat(metadata.getData().getString("name")).isEqualTo("neo-2");

        assertThat(message.getPayload()).isInstanceOf(JsonObject.class);
        assertThat(message.getPayload().getString("name")).isEqualTo("neo-2");
    }

    @Test
    public void testReceivingBinaryCloudEventsWithSupportDisabled() {
        String address = UUID.randomUUID().toString();

        new MapBasedConfig()
                .with("mp.messaging.incoming.source.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.source.address", address)
                .with("mp.messaging.incoming.source.host", host)
                .with("mp.messaging.incoming.source.port", port)
                .with("mp.messaging.incoming.source.cloud-events", false)
                .with("mp.messaging.incoming.source.tracing-enabled", false)
                .with("amqp-username", username)
                .with("amqp-password", password)
                .write();

        weld.addBeanClass(MyConsumptionBean.class);
        container = weld.initialize();
        await().until(() -> isAmqpConnectorReady(container));
        MyConsumptionBean bean = container.getBeanManager().createInstance().select(MyConsumptionBean.class).get();

        usage.produce(address, 1, () -> {
            JsonObject json = new JsonObject()
                    .put(AmqpCloudEventHelper.AMQP_HEADER_FOR_SPEC_VERSION, CloudEventMetadata.CE_VERSION_1_0)
                    .put(AmqpCloudEventHelper.AMQP_HEADER_FOR_TYPE, "type")
                    .put(AmqpCloudEventHelper.AMQP_HEADER_FOR_ID, "id")
                    .put(AmqpCloudEventHelper.AMQP_HEADER_FOR_SOURCE, "test://test")
                    .put(AmqpCloudEventHelper.AMQP_HEADER_FOR_SUBJECT, "foo")
                    .put(AmqpCloudEventHelper.AMQP_HEADER_FOR_SCHEMA, "http://schema.io")
                    .put(AmqpCloudEventHelper.AMQP_HEADER_FOR_TIME, "2020-07-23T09:12:34Z");
            JsonObject payload = new JsonObject();
            payload.put("name", "neo");

            return AmqpMessage.create()
                    .applicationProperties(json)
                    .withJsonObjectAsBody(payload)
                    .build();
        });

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.getList().size() >= 1);
        Message<JsonObject> message = bean.getList().get(0);
        IncomingCloudEventMetadata<JsonObject> metadata = message.getMetadata(IncomingCloudEventMetadata.class)
                .orElse(null);
        IncomingAmqpMetadata amqpMetadata = message.getMetadata(IncomingAmqpMetadata.class).orElse(null);
        assertThat(metadata).isNull();
        assertThat(amqpMetadata).isNotNull();
        assertThat(message.getPayload()).isInstanceOf(JsonObject.class).containsExactly(entry("name", "neo"));
        assertThat(amqpMetadata.getProperties().getString(AmqpCloudEventHelper.AMQP_HEADER_FOR_TYPE)).isEqualTo("type");

    }

    @Test
    public void testReceivingStructuredCloudEventsWithSupportDisabled() {
        String address = UUID.randomUUID().toString();

        new MapBasedConfig()
                .with("mp.messaging.incoming.source.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.source.address", address)
                .with("mp.messaging.incoming.source.host", host)
                .with("mp.messaging.incoming.source.port", port)
                .with("mp.messaging.incoming.source.tracing-enabled", false)
                .with("mp.messaging.incoming.source.cloud-events", false)
                .with("amqp-username", username)
                .with("amqp-password", password)
                .write();

        weld.addBeanClass(MyConsumptionBean.class);
        container = weld.initialize();
        await().until(() -> isAmqpConnectorReady(container));

        MyConsumptionBean bean = container.getBeanManager().createInstance().select(MyConsumptionBean.class).get();

        usage.produce(address, 1, () -> {
            JsonObject json = new JsonObject()
                    .put("specversion", CloudEventMetadata.CE_VERSION_1_0)
                    .put("type", "type")
                    .put("id", "id")
                    .put("source", "test://test")
                    .put("subject", "foo")
                    .put("datacontenttype", "application/json")
                    .put("dataschema", "http://schema.io")
                    .put("time", "2020-07-23T09:12:34Z")
                    .put("data", new JsonObject().put("name", "neo"));

            return AmqpMessage.create()
                    .withJsonObjectAsBody(json)
                    .contentType(AmqpCloudEventHelper.STRUCTURED_CONTENT_TYPE).build();
        });

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.getList().size() >= 1);
        Message<JsonObject> message = bean.getList().get(0);
        IncomingCloudEventMetadata<JsonObject> metadata = message.getMetadata(IncomingCloudEventMetadata.class)
                .orElse(null);
        IncomingAmqpMetadata amqpMetadata = message.getMetadata(IncomingAmqpMetadata.class).orElse(null);
        assertThat(metadata).isNull();
        assertThat(amqpMetadata).isNotNull();

    }

    @Test
    public void testReceivingStructuredCloudEventsNoData() {
        String address = UUID.randomUUID().toString();

        new MapBasedConfig()
                .with("mp.messaging.incoming.source.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.source.address", address)
                .with("mp.messaging.incoming.source.host", host)
                .with("mp.messaging.incoming.source.port", port)
                .with("mp.messaging.incoming.source.tracing-enabled", false)
                .with("amqp-username", username)
                .with("amqp-password", password)
                .write();

        weld.addBeanClass(MyConsumptionBean.class);
        container = weld.initialize();
        await().until(() -> isAmqpConnectorReady(container));
        MyConsumptionBean bean = container.getBeanManager().createInstance().select(MyConsumptionBean.class).get();

        usage.produce(address, 1, () -> {
            JsonObject json = new JsonObject()
                    .put("specversion", CloudEventMetadata.CE_VERSION_1_0)
                    .put("type", "type")
                    .put("id", "id")
                    .put("source", "test://test")
                    .put("subject", "foo")
                    .put("datacontenttype", "application/json")
                    .put("dataschema", "http://schema.io")
                    .put("time", "2020-07-23T09:12:34Z");

            return AmqpMessage.create()
                    .withJsonObjectAsBody(json)
                    .contentType(AmqpCloudEventHelper.STRUCTURED_CONTENT_TYPE).build();
        });

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.getList().size() >= 1);
        Message<JsonObject> message = bean.getList().get(0);
        IncomingCloudEventMetadata<JsonObject> metadata = message.getMetadata(IncomingCloudEventMetadata.class)
                .orElse(null);
        assertThat(metadata).isNotNull();
        assertThat(metadata.getSpecVersion()).isEqualTo(CloudEventMetadata.CE_VERSION_1_0);
        assertThat(metadata.getType()).isEqualTo("type");
        assertThat(metadata.getId()).isEqualTo("id");
        assertThat(metadata.getSource()).isEqualTo(URI.create("test://test"));
        assertThat(metadata.getData()).isNull();

        assertThat(message.getPayload()).isNull();
    }

    @ApplicationScoped
    public static class MyConsumptionBean {

        List<Message<JsonObject>> list = new CopyOnWriteArrayList<>();

        @Incoming("source")
        public CompletionStage<Void> consume(Message<JsonObject> s) {
            list.add(s);
            return s.ack();
        }

        public List<Message<JsonObject>> getList() {
            return list;
        }
    }

    @ApplicationScoped
    public static class MyStringConsumptionBean {

        List<Message<String>> list = new CopyOnWriteArrayList<>();

        @Incoming("source")
        public CompletionStage<Void> consume(Message<String> s) {
            list.add(s);
            return s.ack();
        }

        public List<Message<String>> getList() {
            return list;
        }
    }

}
