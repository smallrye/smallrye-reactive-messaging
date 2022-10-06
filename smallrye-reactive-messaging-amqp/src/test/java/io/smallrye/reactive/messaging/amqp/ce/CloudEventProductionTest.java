package io.smallrye.reactive.messaging.amqp.ce;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.awaitility.Awaitility.await;

import java.net.URI;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.reactive.messaging.*;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.amqp.AmqpBrokerTestBase;
import io.smallrye.reactive.messaging.amqp.AmqpConnector;
import io.smallrye.reactive.messaging.ce.OutgoingCloudEventMetadata;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.amqp.AmqpMessage;

@SuppressWarnings("unchecked")
public class CloudEventProductionTest extends AmqpBrokerTestBase {

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
    public void testSendingStructuredCloudEvents() {
        String address = UUID.randomUUID().toString();

        new MapBasedConfig()
                .with("mp.messaging.outgoing.amqp.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.amqp.address", address)
                .with("mp.messaging.outgoing.amqp.host", host)
                .with("mp.messaging.outgoing.amqp.port", port)
                .with("mp.messaging.outgoing.amqp.tracing-enabled", false)
                .with("mp.messaging.outgoing.amqp.cloud-events-mode", "structured")
                .with("amqp-username", username)
                .with("amqp-password", password)
                .write();

        weld.addBeanClass(AmqpSender.class);
        container = weld.initialize();
        AmqpSender bean = container.getBeanManager().createInstance().select(AmqpSender.class).get();
        Emitter<JsonObject> emitter = bean.get();

        List<io.vertx.mutiny.amqp.AmqpMessage> list = new ArrayList<>();
        usage.consume(address, list::add);

        Message<JsonObject> msg = Message.of(new JsonObject().put("message", "hello"))
                .addMetadata(OutgoingCloudEventMetadata.builder()
                        .withSource(URI.create("test://test"))
                        .withType("type")
                        .withId("some id")
                        .build());

        emitter.send(msg);

        await().until(() -> list.size() == 1);
        AmqpMessage message = list.get(0);
        assertThat(message.address()).isEqualTo(address);
        assertThat(message.contentType()).isEqualTo(AmqpCloudEventHelper.STRUCTURED_CONTENT_TYPE);
        JsonObject json = message.bodyAsJsonObject();
        assertThat(json.getString("specversion")).isEqualTo("1.0");
        assertThat(json.getString("type")).isEqualTo("type");
        assertThat(json.getString("source")).isEqualTo("test://test");
        assertThat(json.getString("id")).isEqualTo("some id");
        assertThat(json.getJsonObject("data").getString("message")).isEqualTo("hello");
    }

    @Test
    public void testSendingStructuredCloudEventsWithTimestampAndSubject() {
        String address = UUID.randomUUID().toString();

        new MapBasedConfig()
                .with("mp.messaging.outgoing.amqp.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.amqp.address", address)
                .with("mp.messaging.outgoing.amqp.host", host)
                .with("mp.messaging.outgoing.amqp.port", port)
                .with("mp.messaging.outgoing.amqp.tracing-enabled", false)
                .with("mp.messaging.outgoing.amqp.cloud-events-mode", "structured")
                .with("amqp-username", username)
                .with("amqp-password", password)
                .write();

        weld.addBeanClass(AmqpSender.class);
        container = weld.initialize();
        AmqpSender bean = container.getBeanManager().createInstance().select(AmqpSender.class).get();
        Emitter<JsonObject> emitter = bean.get();

        List<io.vertx.mutiny.amqp.AmqpMessage> list = new ArrayList<>();
        usage.consume(address, list::add);

        ZonedDateTime time = ZonedDateTime.now();

        Message<JsonObject> msg = Message.of(new JsonObject().put("message", "hello"))
                .addMetadata(OutgoingCloudEventMetadata.builder()
                        .withSource(URI.create("test://test"))
                        .withType("type")
                        .withId("some id")
                        .withTimestamp(time)
                        .withSubject("subject")
                        .build());

        emitter.send(msg);

        await().until(() -> list.size() == 1);
        AmqpMessage message = list.get(0);
        assertThat(message.address()).isEqualTo(address);
        assertThat(message.contentType()).isEqualTo(AmqpCloudEventHelper.STRUCTURED_CONTENT_TYPE);
        JsonObject json = message.bodyAsJsonObject();
        assertThat(json.getString("specversion")).isEqualTo("1.0");
        assertThat(json.getString("type")).isEqualTo("type");
        assertThat(json.getString("source")).isEqualTo("test://test");
        assertThat(json.getString("id")).isEqualTo("some id");
        assertThat(json.getJsonObject("data").getString("message")).isEqualTo("hello");
        assertThat(json.getString("subject")).isEqualTo("subject");
        assertThat(json.getInstant("time")).isNotNull();
        assertThat(json.getInstant("time").getEpochSecond()).isEqualTo(time.toEpochSecond());
    }

    @Test
    public void testSendingStructuredCloudEventsMissingMandatoryAttribute() {
        String address = UUID.randomUUID().toString();

        new MapBasedConfig()
                .with("mp.messaging.outgoing.amqp.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.amqp.address", address)
                .with("mp.messaging.outgoing.amqp.host", host)
                .with("mp.messaging.outgoing.amqp.port", port)
                .with("mp.messaging.outgoing.amqp.tracing-enabled", false)
                .with("mp.messaging.outgoing.amqp.cloud-events-mode", "structured")
                .with("amqp-username", username)
                .with("amqp-password", password)
                .write();

        List<io.vertx.mutiny.amqp.AmqpMessage> list = new ArrayList<>();
        usage.consume(address, list::add);

        weld.addBeanClass(AmqpSender.class);
        container = weld.initialize();
        AmqpSender bean = container.getBeanManager().createInstance().select(AmqpSender.class).get();
        Emitter<JsonObject> emitter = bean.get();

        Message<JsonObject> msg = Message.of(new JsonObject().put("message", "hello"))
                .addMetadata(OutgoingCloudEventMetadata.builder()
                        .withSource(URI.create("test://test"))
                        // type omitted on purpose
                        .withId("some id")
                        .build());

        emitter.send(msg);

        await()
                .pollDelay(Duration.ofSeconds(1))
                .until(() -> list.size() == 0);
    }

    @Test
    public void testSendingStructuredCloudEventsWithConfiguredTypeAndSource() {
        String address = UUID.randomUUID().toString();

        new MapBasedConfig()
                .with("mp.messaging.outgoing.amqp.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.amqp.address", address)
                .with("mp.messaging.outgoing.amqp.host", host)
                .with("mp.messaging.outgoing.amqp.port", port)
                .with("mp.messaging.outgoing.amqp.tracing-enabled", false)
                .with("mp.messaging.outgoing.amqp.cloud-events-mode", "structured")
                .with("mp.messaging.outgoing.amqp.cloud-events-type", "my type")
                .with("mp.messaging.outgoing.amqp.cloud-events-source", "http://acme.org")
                .with("amqp-username", username)
                .with("amqp-password", password)
                .write();

        weld.addBeanClass(AmqpSender.class);
        container = weld.initialize();
        AmqpSender bean = container.getBeanManager().createInstance().select(AmqpSender.class).get();
        Emitter<JsonObject> emitter = bean.get();

        List<io.vertx.mutiny.amqp.AmqpMessage> list = new ArrayList<>();
        usage.consume(address, list::add);

        Message<JsonObject> msg = Message.of(new JsonObject().put("message", "hello"))
                .addMetadata(OutgoingCloudEventMetadata.builder()
                        .withId("some id")
                        .build());

        emitter.send(msg);

        await().until(() -> list.size() == 1);
        AmqpMessage message = list.get(0);
        assertThat(message.address()).isEqualTo(address);
        assertThat(message.contentType()).isEqualTo(AmqpCloudEventHelper.STRUCTURED_CONTENT_TYPE);
        JsonObject json = message.bodyAsJsonObject();
        assertThat(json.getString("specversion")).isEqualTo("1.0");
        assertThat(json.getString("type")).isEqualTo("my type");
        assertThat(json.getString("source")).isEqualTo("http://acme.org");
        assertThat(json.getString("id")).isEqualTo("some id");
    }

    @Test
    public void testSendingStructuredCloudEventsWithExtensions() {
        String address = UUID.randomUUID().toString();

        new MapBasedConfig()
                .with("mp.messaging.outgoing.amqp.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.amqp.address", address)
                .with("mp.messaging.outgoing.amqp.host", host)
                .with("mp.messaging.outgoing.amqp.port", port)
                .with("mp.messaging.outgoing.amqp.tracing-enabled", false)
                .with("mp.messaging.outgoing.amqp.cloud-events-mode", "structured")
                .with("amqp-username", username)
                .with("amqp-password", password)
                .write();

        weld.addBeanClass(AmqpSender.class);
        container = weld.initialize();
        AmqpSender bean = container.getBeanManager().createInstance().select(AmqpSender.class).get();
        Emitter<JsonObject> emitter = bean.get();

        List<io.vertx.mutiny.amqp.AmqpMessage> list = new ArrayList<>();
        usage.consume(address, list::add);

        Message<JsonObject> msg = Message.of(new JsonObject().put("h", "m")).addMetadata(OutgoingCloudEventMetadata.builder()
                .withSource(URI.create("test://test"))
                .withType("type")
                .withId("some id")
                .withExtension("ext", 123)
                .withExtension("ext2", "dddd")
                .build());

        emitter.send(msg);

        await().until(() -> list.size() == 1);
        AmqpMessage message = list.get(0);
        assertThat(message.address()).isEqualTo(address);
        assertThat(message.contentType()).isEqualTo(AmqpCloudEventHelper.STRUCTURED_CONTENT_TYPE);
        JsonObject json = message.bodyAsJsonObject();
        assertThat(json.getString("specversion")).isEqualTo("1.0");
        assertThat(json.getString("type")).isEqualTo("type");
        assertThat(json.getString("source")).isEqualTo("test://test");
        assertThat(json.getString("id")).isEqualTo("some id");
        assertThat(json.getString("ext2")).isEqualTo("dddd");
        assertThat(json.getInteger("ext")).isEqualTo(123);
        assertThat(json.getJsonObject("data")).containsExactly(entry("h", "m"));
    }

    @Test
    public void testSendingBinaryCloudEvents() {
        String address = UUID.randomUUID().toString();

        new MapBasedConfig()
                .with("mp.messaging.outgoing.amqp.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.amqp.address", address)
                .with("mp.messaging.outgoing.amqp.host", host)
                .with("mp.messaging.outgoing.amqp.port", port)
                .with("mp.messaging.outgoing.amqp.tracing-enabled", false)
                .with("amqp-username", username)
                .with("amqp-password", password)
                .write();

        weld.addBeanClass(AmqpSender.class);
        container = weld.initialize();
        AmqpSender bean = container.getBeanManager().createInstance().select(AmqpSender.class).get();
        Emitter<JsonObject> emitter = bean.get();

        List<io.vertx.mutiny.amqp.AmqpMessage> list = new ArrayList<>();
        usage.consume(address, list::add);

        Message<JsonObject> msg = Message.of(new JsonObject().put("message", "hello"))
                .addMetadata(OutgoingCloudEventMetadata.builder()
                        .withSource(URI.create("test://test"))
                        .withType("type")
                        .withId("some id")
                        .build());

        emitter.send(msg);

        await().until(() -> list.size() == 1);
        AmqpMessage message = list.get(0);
        assertThat(message.address()).isEqualTo(address);
        assertThat(message.contentType()).isEqualTo("application/json");
        JsonObject app = message.applicationProperties();
        assertThat(app.getString(AmqpCloudEventHelper.AMQP_HEADER_FOR_SPEC_VERSION)).isEqualTo("1.0");
        assertThat(app.getString(AmqpCloudEventHelper.AMQP_HEADER_FOR_TYPE)).isEqualTo("type");
        assertThat(app.getString(AmqpCloudEventHelper.AMQP_HEADER_FOR_SOURCE)).isEqualTo("test://test");
        assertThat(app.getString(AmqpCloudEventHelper.AMQP_HEADER_FOR_ID)).isEqualTo("some id");
        assertThat(app.getString(AmqpCloudEventHelper.AMQP_HEADER_FOR_CONTENT_TYPE)).isEqualTo("application/json");
        JsonObject body = message.bodyAsJsonObject();
        assertThat(body.getString("message")).isEqualTo("hello");
    }

    @Test
    public void testSendingBinaryCloudEventsWithConfiguredTypeAndSource() {
        String address = UUID.randomUUID().toString();

        new MapBasedConfig()
                .with("mp.messaging.outgoing.amqp.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.amqp.address", address)
                .with("mp.messaging.outgoing.amqp.host", host)
                .with("mp.messaging.outgoing.amqp.port", port)
                .with("mp.messaging.outgoing.amqp.cloud-events-type", "my type")
                .with("mp.messaging.outgoing.amqp.cloud-events-source", "http://acme.org")
                .with("amqp-username", username)
                .with("amqp-password", password)
                .write();

        weld.addBeanClass(AmqpSender.class);
        container = weld.initialize();
        AmqpSender bean = container.getBeanManager().createInstance().select(AmqpSender.class).get();
        Emitter<JsonObject> emitter = bean.get();

        List<io.vertx.mutiny.amqp.AmqpMessage> list = new ArrayList<>();
        usage.consume(address, list::add);

        Message<JsonObject> msg = Message.of(new JsonObject().put("message", "hello"))
                .addMetadata(OutgoingCloudEventMetadata.builder()
                        .withDataContentType("application/json+neo")
                        .withId("some id")
                        .build());

        emitter.send(msg);

        await().until(() -> list.size() == 1);
        AmqpMessage message = list.get(0);
        assertThat(message.address()).isEqualTo(address);
        assertThat(message.contentType()).isEqualTo("application/json+neo");
        JsonObject app = message.applicationProperties();
        assertThat(app.getString(AmqpCloudEventHelper.AMQP_HEADER_FOR_SPEC_VERSION)).isEqualTo("1.0");
        assertThat(app.getString(AmqpCloudEventHelper.AMQP_HEADER_FOR_TYPE)).isEqualTo("my type");
        assertThat(app.getString(AmqpCloudEventHelper.AMQP_HEADER_FOR_SOURCE)).isEqualTo("http://acme.org");
        assertThat(app.getString(AmqpCloudEventHelper.AMQP_HEADER_FOR_ID)).isEqualTo("some id");
        assertThat(app.getString(AmqpCloudEventHelper.AMQP_HEADER_FOR_CONTENT_TYPE)).isEqualTo("application/json+neo");
        JsonObject body = message.bodyAsJsonObject();
        assertThat(body.getString("message")).isEqualTo("hello");
    }

    @Test
    public void testSendingBinaryCloudEventsWithConfiguredTypeAndSourceButNoMetadata() {
        String address = UUID.randomUUID().toString();

        new MapBasedConfig()
                .with("mp.messaging.outgoing.amqp.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.amqp.address", address)
                .with("mp.messaging.outgoing.amqp.host", host)
                .with("mp.messaging.outgoing.amqp.port", port)
                .with("mp.messaging.outgoing.amqp.cloud-events-type", "my type")
                .with("mp.messaging.outgoing.amqp.cloud-events-source", "http://acme.org")
                .with("amqp-username", username)
                .with("amqp-password", password)
                .write();

        weld.addBeanClass(AmqpSender.class);
        container = weld.initialize();
        AmqpSender bean = container.getBeanManager().createInstance().select(AmqpSender.class).get();
        Emitter<JsonObject> emitter = bean.get();

        List<io.vertx.mutiny.amqp.AmqpMessage> list = new ArrayList<>();
        usage.consume(address, list::add);

        Message<JsonObject> msg = Message.of(new JsonObject().put("message", "hello"));

        emitter.send(msg);

        await().until(() -> list.size() == 1);
        AmqpMessage message = list.get(0);
        assertThat(message.address()).isEqualTo(address);
        assertThat(message.contentType()).isEqualTo("application/json");
        JsonObject app = message.applicationProperties();
        assertThat(app.getString(AmqpCloudEventHelper.AMQP_HEADER_FOR_SPEC_VERSION)).isEqualTo("1.0");
        assertThat(app.getString(AmqpCloudEventHelper.AMQP_HEADER_FOR_TYPE)).isEqualTo("my type");
        assertThat(app.getString(AmqpCloudEventHelper.AMQP_HEADER_FOR_SOURCE)).isEqualTo("http://acme.org");
        assertThat(app.getString(AmqpCloudEventHelper.AMQP_HEADER_FOR_ID)).isNotNull();
        assertThat(app.getString(AmqpCloudEventHelper.AMQP_HEADER_FOR_CONTENT_TYPE)).isEqualTo("application/json");
        JsonObject body = message.bodyAsJsonObject();
        assertThat(body.getString("message")).isEqualTo("hello");
    }

    @Test
    public void testSendingBinaryCloudEventsMissingMandatoryAttribute() {
        String address = UUID.randomUUID().toString();

        new MapBasedConfig()
                .with("mp.messaging.outgoing.amqp.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.amqp.address", address)
                .with("mp.messaging.outgoing.amqp.host", host)
                .with("mp.messaging.outgoing.amqp.port", port)
                .with("mp.messaging.outgoing.amqp.tracing-enabled", false)
                .with("amqp-username", username)
                .with("amqp-password", password)
                .write();

        List<io.vertx.mutiny.amqp.AmqpMessage> list = new ArrayList<>();
        usage.consume(address, list::add);

        weld.addBeanClass(AmqpSender.class);
        container = weld.initialize();
        AmqpSender bean = container.getBeanManager().createInstance().select(AmqpSender.class).get();
        Emitter<JsonObject> emitter = bean.get();

        Message<JsonObject> msg = Message.of(new JsonObject().put("message", "hello"))
                .addMetadata(OutgoingCloudEventMetadata.builder()
                        .withSource(URI.create("test://test"))
                        // type omitted on purpose
                        .withId("some id")
                        .build());

        await().until(() -> super.isAmqpConnectorAlive(container));

        emitter.send(msg);

        await()
                .pollDelay(Duration.ofSeconds(1))
                .until(() -> list.size() == 0);
    }

    @Test
    public void testWithCloudEventDisabled() {
        String address = UUID.randomUUID().toString();

        new MapBasedConfig()
                .with("mp.messaging.outgoing.amqp.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.amqp.address", address)
                .with("mp.messaging.outgoing.amqp.host", host)
                .with("mp.messaging.outgoing.amqp.port", port)
                .with("mp.messaging.outgoing.amqp.tracing-enabled", false)
                .with("mp.messaging.outgoing.amqp.cloud-events", false)
                .with("amqp-username", username)
                .with("amqp-password", password)
                .write();

        weld.addBeanClass(AmqpSender.class);
        container = weld.initialize();
        AmqpSender bean = container.getBeanManager().createInstance().select(AmqpSender.class).get();
        Emitter<JsonObject> emitter = bean.get();

        List<io.vertx.mutiny.amqp.AmqpMessage> list = new ArrayList<>();
        usage.consume(address, list::add);

        Message<JsonObject> msg = Message.of(new JsonObject().put("message", "hello"))
                .addMetadata(OutgoingCloudEventMetadata.builder()
                        .withSource(URI.create("test://test"))
                        // We don't care not having the type, cloud events are disabled
                        .withId("some id")
                        .build());

        emitter.send(msg);

        await().until(() -> list.size() == 1);
        AmqpMessage message = list.get(0);
        assertThat(message.address()).isEqualTo(address);
        assertThat(message.contentType()).isEqualTo("application/json");
        JsonObject app = message.applicationProperties();
        assertThat(app).isNull();
        JsonObject body = message.bodyAsJsonObject();
        assertThat(body.getString("message")).isEqualTo("hello");
    }

    @Test
    public void testSendingBinaryCloudEventsWithExtensions() {
        String address = UUID.randomUUID().toString();

        new MapBasedConfig()
                .with("mp.messaging.outgoing.amqp.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.amqp.address", address)
                .with("mp.messaging.outgoing.amqp.host", host)
                .with("mp.messaging.outgoing.amqp.port", port)
                .with("mp.messaging.outgoing.amqp.tracing-enabled", false)
                .with("amqp-username", username)
                .with("amqp-password", password)
                .write();

        weld.addBeanClass(AmqpSender.class);
        container = weld.initialize();
        AmqpSender bean = container.getBeanManager().createInstance().select(AmqpSender.class).get();
        Emitter<JsonObject> emitter = bean.get();

        List<io.vertx.mutiny.amqp.AmqpMessage> list = new ArrayList<>();
        usage.consume(address, list::add);

        Message<JsonObject> msg = Message.of(new JsonObject().put("message", "hello"))
                .addMetadata(OutgoingCloudEventMetadata.builder()
                        .withSource(URI.create("test://test"))
                        .withType("type")
                        .withId("some id")
                        .withExtension("ext", 124)
                        .withExtension("ext2", "bonjour")
                        .build());

        emitter.send(msg);

        await().until(() -> list.size() == 1);
        AmqpMessage message = list.get(0);
        assertThat(message.address()).isEqualTo(address);
        assertThat(message.contentType()).isEqualTo("application/json");
        JsonObject app = message.applicationProperties();
        assertThat(app.getString(AmqpCloudEventHelper.AMQP_HEADER_FOR_SPEC_VERSION)).isEqualTo("1.0");
        assertThat(app.getString(AmqpCloudEventHelper.AMQP_HEADER_FOR_TYPE)).isEqualTo("type");
        assertThat(app.getString(AmqpCloudEventHelper.AMQP_HEADER_FOR_SOURCE)).isEqualTo("test://test");
        assertThat(app.getString(AmqpCloudEventHelper.AMQP_HEADER_FOR_ID)).isEqualTo("some id");
        assertThat(app.getString(AmqpCloudEventHelper.AMQP_HEADER_FOR_CONTENT_TYPE)).isEqualTo("application/json");
        assertThat(app.getInteger(AmqpCloudEventHelper.CE_HEADER_PREFIX + "ext")).isEqualTo(124);
        assertThat(app.getString(AmqpCloudEventHelper.CE_HEADER_PREFIX + "ext2")).isEqualTo("bonjour");
        JsonObject body = message.bodyAsJsonObject();
        assertThat(body.getString("message")).isEqualTo("hello");
    }

    @Test
    public void testSendingStructuredCloudEventFromBean() {
        String address = UUID.randomUUID().toString();

        new MapBasedConfig()
                .with("mp.messaging.outgoing.amqp.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.amqp.address", address)
                .with("mp.messaging.outgoing.amqp.host", host)
                .with("mp.messaging.outgoing.amqp.port", port)
                .with("mp.messaging.outgoing.amqp.tracing-enabled", false)
                .with("mp.messaging.outgoing.amqp.cloud-events-mode", "structured")
                .with("amqp-username", username)
                .with("amqp-password", password)
                .write();

        List<io.vertx.mutiny.amqp.AmqpMessage> list = new ArrayList<>();
        usage.consume(address, list::add);

        weld.addBeanClasses(Source.class, Processing.class, Sender.class);
        container = weld.initialize();

        await().until(() -> list.size() >= 10);

        assertThat(list).allSatisfy(message -> {
            assertThat(message.address()).isEqualTo(address);
            assertThat(message.contentType()).isEqualTo(AmqpCloudEventHelper.STRUCTURED_CONTENT_TYPE);
            JsonObject json = message.bodyAsJsonObject();
            assertThat(json.getString("specversion")).isEqualTo("1.0");
            assertThat(json.getString("type")).isEqualTo("greeting");
            assertThat(json.getString("source")).isEqualTo("source://me");
            assertThat(json.getString("id")).startsWith("id-hello-");
            assertThat(json.getString("data")).startsWith("hello-");
        });

    }

    @Test
    public void testSendingBinaryCloudEventFromBean() {
        String address = UUID.randomUUID().toString();

        new MapBasedConfig()
                .with("mp.messaging.outgoing.amqp.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.amqp.address", address)
                .with("mp.messaging.outgoing.amqp.host", host)
                .with("mp.messaging.outgoing.amqp.port", port)
                .with("mp.messaging.outgoing.amqp.tracing-enabled", false)
                .with("amqp-username", username)
                .with("amqp-password", password)
                .write();

        List<io.vertx.mutiny.amqp.AmqpMessage> list = new ArrayList<>();
        usage.consume(address, list::add);

        weld.addBeanClasses(Source.class, Processing.class, Sender.class);
        container = weld.initialize();

        await().until(() -> list.size() >= 10);

        assertThat(list).allSatisfy(message -> {
            assertThat(message.address()).isEqualTo(address);
            JsonObject app = message.applicationProperties();
            String value = message.bodyAsString();
            assertThat(app.getString(AmqpCloudEventHelper.AMQP_HEADER_FOR_SPEC_VERSION)).isEqualTo("1.0");
            assertThat(app.getString(AmqpCloudEventHelper.AMQP_HEADER_FOR_TYPE)).isEqualTo("greeting");
            assertThat(app.getString(AmqpCloudEventHelper.AMQP_HEADER_FOR_SOURCE)).isEqualTo("source://me");
            assertThat(app.getString(AmqpCloudEventHelper.AMQP_HEADER_FOR_ID)).startsWith("id-hello-");
            assertThat(value).startsWith("hello-");
        });

    }

    @Test
    public void testSendingBinaryCloudEventFromBeanWithDefault() {
        String address = UUID.randomUUID().toString();

        new MapBasedConfig()
                .with("mp.messaging.outgoing.source.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.source.address", address)
                .with("mp.messaging.outgoing.source.host", host)
                .with("mp.messaging.outgoing.source.port", port)
                .with("mp.messaging.outgoing.source.tracing-enabled", false)
                .with("mp.messaging.outgoing.source.cloud-events-type", "greetings")
                .with("mp.messaging.outgoing.source.cloud-events-source", "source://me")
                .with("mp.messaging.outgoing.source.cloud-events-subject", "test")
                .with("amqp-username", username)
                .with("amqp-password", password)
                .write();

        List<io.vertx.mutiny.amqp.AmqpMessage> list = new ArrayList<>();
        usage.consume(address, list::add);

        weld.addBeanClasses(Source.class);
        container = weld.initialize();

        await().until(() -> list.size() >= 10);

        assertThat(list).allSatisfy(message -> {
            assertThat(message.address()).isEqualTo(address);
            JsonObject app = message.applicationProperties();
            String value = message.bodyAsString();
            assertThat(app.getString(AmqpCloudEventHelper.AMQP_HEADER_FOR_SPEC_VERSION)).isEqualTo("1.0");
            assertThat(app.getString(AmqpCloudEventHelper.AMQP_HEADER_FOR_TYPE)).isEqualTo("greetings");
            assertThat(app.getString(AmqpCloudEventHelper.AMQP_HEADER_FOR_SOURCE)).isEqualTo("source://me");
            assertThat(app.getString(AmqpCloudEventHelper.AMQP_HEADER_FOR_ID)).isNotNull();
            assertThat(value).startsWith("hello-");
        });

    }

    @Test
    public void testSendingStructuredCloudEventFromBeanWithDefault() {
        String address = UUID.randomUUID().toString();

        new MapBasedConfig()
                .with("mp.messaging.outgoing.source.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.source.address", address)
                .with("mp.messaging.outgoing.source.host", host)
                .with("mp.messaging.outgoing.source.port", port)
                .with("mp.messaging.outgoing.source.tracing-enabled", false)
                .with("mp.messaging.outgoing.source.cloud-events-mode", "structured")
                .with("mp.messaging.outgoing.source.cloud-events-type", "greetings")
                .with("mp.messaging.outgoing.source.cloud-events-source", "source://me")
                .with("mp.messaging.outgoing.source.cloud-events-subject", "test")
                .with("amqp-username", username)
                .with("amqp-password", password)
                .write();

        List<io.vertx.mutiny.amqp.AmqpMessage> list = new ArrayList<>();
        usage.consume(address, list::add);

        weld.addBeanClasses(Source.class);
        container = weld.initialize();

        await().until(() -> list.size() >= 10);

        assertThat(list).allSatisfy(message -> {
            assertThat(message.address()).isEqualTo(address);
            JsonObject json = message.bodyAsJsonObject();
            assertThat(json.getString("specversion")).isEqualTo("1.0");
            assertThat(json.getString("type")).isEqualTo("greetings");
            assertThat(json.getString("source")).isEqualTo("source://me");
            assertThat(json.getString("subject")).isEqualTo("test");
            assertThat(json.getString("id")).isNotNull();
            assertThat(json.getInstant("time")).isNotNull();
            assertThat(json.getString("data")).startsWith("hello-");
        });

    }

    @ApplicationScoped
    public static class AmqpSender {

        @Inject
        @Channel("amqp")
        Emitter<JsonObject> emitter;

        public Emitter<JsonObject> get() {
            return emitter;
        }

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
        @Outgoing("amqp")
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
