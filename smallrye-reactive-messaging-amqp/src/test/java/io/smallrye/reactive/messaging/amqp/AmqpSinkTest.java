package io.smallrye.reactive.messaging.amqp;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.core.Is.is;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorFactory;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.jboss.weld.exceptions.DeploymentException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;

import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.amqp.AmqpMessageBuilder;
import io.vertx.mutiny.core.buffer.Buffer;

public class AmqpSinkTest extends AmqpBrokerTestBase {

    private static final String HELLO = "hello-";
    private WeldContainer container;
    private AmqpConnector provider;

    @AfterEach
    public void cleanup() {
        if (provider != null) {
            provider.terminate(null);
        }

        if (container != null) {
            container.shutdown();
        }

        MapBasedConfig.clear();
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
    }

    @Test
    public void testSinkUsingInteger() {
        String topic = UUID.randomUUID().toString();
        AtomicInteger expected = new AtomicInteger(0);
        usage.consumeIntegers(topic,
                v -> expected.getAndIncrement());

        SubscriberBuilder<? extends Message<?>, Void> sink = createProviderAndSink(topic);
        //noinspection unchecked
        Multi.createFrom().range(0, 10)
                .map(Message::of)
                .subscribe((Subscriber<? super Message<Integer>>) sink.build());

        await().until(() -> expected.get() == 10);
        assertThat(expected).hasValue(10);
    }

    @Test
    public void testSinkUsingIntegerUsingNonAnonymousSender() {
        String topic = UUID.randomUUID().toString();
        AtomicInteger expected = new AtomicInteger(0);
        usage.consumeIntegers(topic,
                v -> expected.getAndIncrement());

        SubscriberBuilder<? extends Message<?>, Void> sink = createProviderAndNonAnonymousSink(topic);
        //noinspection unchecked
        Multi.createFrom().range(0, 10)
                .map(Message::of)
                .subscribe((Subscriber<? super Message<Integer>>) sink.build());

        await().until(() -> expected.get() == 10);
        assertThat(expected).hasValue(10);
    }

    @Test
    public void testSinkUsingString() {
        String topic = UUID.randomUUID().toString();

        SubscriberBuilder<? extends Message<?>, Void> sink = createProviderAndSink(topic);

        AtomicInteger expected = new AtomicInteger(0);
        usage.consumeStrings(topic,
                v -> expected.getAndIncrement());

        //noinspection unchecked
        Multi.createFrom().range(0, 10)
                .map(i -> Integer.toString(i))
                .map(Message::of)
                .subscribe((Subscriber<? super Message<String>>) sink.build());

        await().untilAtomic(expected, is(10));
        assertThat(expected).hasValue(10);
    }

    static class Person {
        String name;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    @Test
    public void testSinkUsingObject() {
        String topic = UUID.randomUUID().toString();

        SubscriberBuilder<? extends Message<?>, Void> sink = createProviderAndSink(topic);

        AtomicInteger expected = new AtomicInteger(0);
        usage.consume(topic,
                v -> {
                    expected.getAndIncrement();
                    Person p = v.bodyAsBinary().toJsonObject().mapTo(Person.class);
                    assertThat(p.getName()).startsWith("bob-");
                });

        //noinspection unchecked
        Multi.createFrom().range(0, 10)
                .map(i -> {
                    Person p = new Person();
                    p.setName("bob-" + i);
                    return p;
                })
                .map(Message::of)
                .subscribe((Subscriber<? super Message<Person>>) sink.build());

        await().untilAtomic(expected, is(10));
        assertThat(expected).hasValue(10);
    }

    @Test
    public void testSinkUsingJsonObject() {
        String topic = UUID.randomUUID().toString();

        SubscriberBuilder<? extends Message<?>, Void> sink = createProviderAndSink(topic);

        AtomicInteger expected = new AtomicInteger(0);
        usage.consume(topic,
                v -> {
                    expected.getAndIncrement();
                    assertThat(v.bodyAsJsonObject().getString("id")).startsWith("bob-");
                    assertThat(v.contentType()).isEqualTo("application/json");
                });

        //noinspection unchecked
        Multi.createFrom().range(0, 10)
                .map(i -> new JsonObject().put("id", "bob-" + i))
                .map(Message::of)
                .subscribe((Subscriber<? super Message<?>>) sink.build());

        await().untilAtomic(expected, is(10));
        assertThat(expected).hasValue(10);
    }

    @Test
    public void testSinkUsingJsoArray() {
        String topic = UUID.randomUUID().toString();

        SubscriberBuilder<? extends Message<?>, Void> sink = createProviderAndSink(topic);

        AtomicInteger expected = new AtomicInteger(0);
        usage.consume(topic,
                v -> {
                    expected.getAndIncrement();
                    assertThat(v.bodyAsJsonArray().getString(0)).startsWith("bob-");
                    assertThat(v.bodyAsJsonArray().getString(1)).isEqualTo("foo");
                    assertThat(v.contentType()).isEqualTo("application/json");
                });

        //noinspection unchecked
        Multi.createFrom().range(0, 10)
                .map(i -> new JsonArray().add("bob-" + i).add("foo"))
                .map(Message::of)
                .subscribe((Subscriber<? super Message<?>>) sink.build());

        await().untilAtomic(expected, is(10));
        assertThat(expected).hasValue(10);
    }

    @Test
    public void testSinkUsingBuffer() {
        String topic = UUID.randomUUID().toString();

        SubscriberBuilder<? extends Message<?>, Void> sink = createProviderAndSink(topic);

        AtomicInteger expected = new AtomicInteger(0);
        usage.consume(topic,
                v -> {
                    expected.getAndIncrement();
                    assertThat(v.bodyAsJsonObject().getString("id")).startsWith("bob-");
                    assertThat(v.contentType()).isEqualTo("application/octet-stream");
                });

        //noinspection unchecked
        Multi.createFrom().range(0, 10)
                .map(i -> new JsonObject().put("id", "bob-" + i).toBuffer())
                .map(Message::of)
                .subscribe((Subscriber<? super Message<?>>) sink.build());

        await().untilAtomic(expected, is(10));
        assertThat(expected).hasValue(10);
    }

    @Test
    public void testSinkUsingByteArray() {
        String topic = UUID.randomUUID().toString();

        SubscriberBuilder<? extends Message<?>, Void> sink = createProviderAndSink(topic);

        AtomicInteger expected = new AtomicInteger(0);
        usage.consume(topic,
                v -> {
                    expected.getAndIncrement();
                    assertThat(v.bodyAsJsonObject().getString("id")).startsWith("bob-");
                    assertThat(v.contentType()).isEqualTo("application/octet-stream");
                });

        //noinspection unchecked
        Multi.createFrom().range(0, 10)
                .map(i -> new JsonObject().put("id", "bob-" + i).toBuffer().getBytes())
                .map(Message::of)
                .subscribe((Subscriber<? super Message<?>>) sink.build());

        await().untilAtomic(expected, is(10));
        assertThat(expected).hasValue(10);
    }

    @Test
    public void testSinkUsingMutinyBuffer() {
        String topic = UUID.randomUUID().toString();

        SubscriberBuilder<? extends Message<?>, Void> sink = createProviderAndSink(topic);

        AtomicInteger expected = new AtomicInteger(0);
        usage.consume(topic,
                v -> {
                    expected.getAndIncrement();
                    assertThat(v.bodyAsJsonObject().getString("id")).startsWith("bob-");
                    assertThat(v.contentType()).isEqualTo("application/octet-stream");
                });

        //noinspection unchecked
        Multi.createFrom().range(0, 10)
                .map(i -> new Buffer(new JsonObject().put("id", "bob-" + i).toBuffer()))
                .map(Message::of)
                .subscribe((Subscriber<? super Message<?>>) sink.build());

        await().untilAtomic(expected, is(10));
        assertThat(expected).hasValue(10);
    }

    @Test
    public void testABeanProducingMessagesSentToAMQP() throws InterruptedException {
        Weld weld = new Weld();

        CountDownLatch latch = new CountDownLatch(10);
        usage.consumeIntegers("sink",
                v -> latch.countDown());

        weld.addBeanClass(ProducingBean.class);

        new MapBasedConfig()
                .put("mp.messaging.outgoing.sink.address", "sink")
                .put("mp.messaging.outgoing.sink.connector", AmqpConnector.CONNECTOR_NAME)
                .put("mp.messaging.outgoing.sink.host", host)
                .put("mp.messaging.outgoing.sink.port", port)
                .put("mp.messaging.outgoing.sink.durable", false)
                .put("amqp-username", username)
                .put("amqp-password", password)
                .write();

        container = weld.initialize();

        assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    public void testABeanProducingMessagesSentToAMQPWithOutboundMetadata() throws InterruptedException {
        Weld weld = new Weld();

        CountDownLatch latch = new CountDownLatch(10);
        usage.consumeIntegers("sink",
                v -> latch.countDown());

        weld.addBeanClass(ProducingBeanUsingOutboundMetadata.class);

        new MapBasedConfig()
                .put("mp.messaging.outgoing.sink.address", "not-used")
                .put("mp.messaging.outgoing.sink.connector", AmqpConnector.CONNECTOR_NAME)
                .put("mp.messaging.outgoing.sink.host", host)
                .put("mp.messaging.outgoing.sink.port", port)
                .put("mp.messaging.outgoing.sink.durable", false)
                .put("amqp-username", username)
                .put("amqp-password", password)
                .write();

        container = weld.initialize();

        assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    public void testABeanProducingMessagesSentToAMQPWithOutboundMetadataUsingNonAnonymousSender()
            throws InterruptedException {
        Weld weld = new Weld();

        CountDownLatch latch = new CountDownLatch(10);
        usage.consumeIntegers("sink-foo",
                v -> latch.countDown());

        weld.addBeanClass(ProducingBeanUsingOutboundMetadata.class);

        new MapBasedConfig()
                .put("mp.messaging.outgoing.sink.address", "sink-foo")
                .put("mp.messaging.outgoing.sink.connector", AmqpConnector.CONNECTOR_NAME)
                .put("mp.messaging.outgoing.sink.host", host)
                .put("mp.messaging.outgoing.sink.port", port)
                .put("mp.messaging.outgoing.sink.durable", false)
                .put("mp.messaging.outgoing.sink.use-anonymous-sender", false)
                .put("amqp-username", username)
                .put("amqp-password", password)
                .write();

        container = weld.initialize();

        assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testSinkUsingAmqpMessage() {
        String topic = UUID.randomUUID().toString();
        AtomicInteger expected = new AtomicInteger(0);

        List<AmqpMessage<String>> messages = new ArrayList<>();
        usage.consume(topic,
                v -> {
                    expected.getAndIncrement();
                    v.getDelegate().accepted();
                    messages.add(new AmqpMessage<>(v, null, null));
                });

        SubscriberBuilder<? extends Message<?>, Void> sink = createProviderAndSink(topic);

        //noinspection unchecked
        Multi.createFrom().range(0, 10)
                .map(v -> AmqpMessage.<String> builder()
                        .withBody(HELLO + v)
                        .withSubject("foo")
                        .build())
                .subscribe((Subscriber<? super AmqpMessage<String>>) sink.build());

        await().untilAtomic(expected, is(10));
        assertThat(expected).hasValue(10);

        messages.forEach(m -> {
            assertThat(m.getPayload()).isInstanceOf(String.class).startsWith(HELLO);
            assertThat(m.getSubject()).isEqualTo("foo");
        });
    }

    @Test
    public void testSinkUsingProtonMessage() {
        String topic = UUID.randomUUID().toString();
        AtomicInteger expected = new AtomicInteger(0);

        List<AmqpMessage<String>> messages = new ArrayList<>();
        usage.consume(topic,
                v -> {
                    expected.getAndIncrement();
                    v.getDelegate().accepted();
                    messages.add(new AmqpMessage<>(v, null, null));
                });

        SubscriberBuilder<? extends Message<?>, Void> sink = createProviderAndSink(topic);

        //noinspection unchecked
        Multi.createFrom().range(0, 10)
                .map(v -> {
                    org.apache.qpid.proton.message.Message message = org.apache.qpid.proton.message.Message.Factory
                            .create();
                    message.setBody(new AmqpValue(HELLO + v));
                    message.setSubject("bar");
                    message.setContentType("text/plain");
                    return Message.of(message);
                })
                .subscribe((Subscriber<? super Message<?>>) sink.build());

        await().untilAtomic(expected, is(10));
        assertThat(expected).hasValue(10);

        messages.forEach(m -> {
            assertThat(m.getPayload()).isInstanceOf(String.class).startsWith(HELLO);
            assertThat(m.getSubject()).isEqualTo("bar");
            assertThat(m.getContentType()).isEqualTo("text/plain");
        });
    }

    @Test
    public void testSinkUsingMutinyMessage() {
        String topic = UUID.randomUUID().toString();
        AtomicInteger expected = new AtomicInteger(0);

        List<AmqpMessage<String>> messages = new ArrayList<>();
        usage.consume(topic,
                v -> {
                    expected.getAndIncrement();
                    v.getDelegate().accepted();
                    messages.add(new AmqpMessage<>(v, null, null));
                });

        SubscriberBuilder<? extends Message<?>, Void> sink = createProviderAndSink(topic);

        //noinspection unchecked
        Multi.createFrom().range(0, 10)
                .map(v -> {
                    AmqpMessageBuilder builder = AmqpMessageBuilder.create();
                    builder.subject("baz")
                            .withBody(HELLO + v)
                            .contentType("text/plain");
                    return Message.of(builder.build());
                })
                .subscribe((Subscriber<? super Message<?>>) sink.build());

        await().untilAtomic(expected, is(10));
        assertThat(expected).hasValue(10);

        messages.forEach(m -> {
            assertThat(m.getPayload()).isInstanceOf(String.class).startsWith(HELLO);
            assertThat(m.getSubject()).isEqualTo("baz");
            assertThat(m.getContentType()).isEqualTo("text/plain");
        });
    }

    @Test
    public void testSinkUsingBareVertxMessage() {
        String topic = UUID.randomUUID().toString();
        AtomicInteger expected = new AtomicInteger(0);

        List<AmqpMessage<String>> messages = new ArrayList<>();
        usage.consume(topic,
                v -> {
                    expected.getAndIncrement();
                    v.getDelegate().accepted();
                    messages.add(new AmqpMessage<>(v, null, null));
                });

        SubscriberBuilder<? extends Message<?>, Void> sink = createProviderAndSink(topic);

        //noinspection unchecked
        Multi.createFrom().range(0, 10)
                .map(v -> {
                    io.vertx.amqp.AmqpMessageBuilder builder = io.vertx.amqp.AmqpMessageBuilder.create();
                    builder.subject("baz")
                            .withBody(HELLO + v)
                            .contentType("text/plain");
                    return Message.of(builder.build());
                })
                .subscribe((Subscriber<? super Message<?>>) sink.build());

        await().untilAtomic(expected, is(10));
        assertThat(expected).hasValue(10);

        messages.forEach(m -> {
            assertThat(m.getPayload()).isInstanceOf(String.class).startsWith(HELLO);
            assertThat(m.getSubject()).isEqualTo("baz");
            assertThat(m.getContentType()).isEqualTo("text/plain");
        });
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testSinkUsingAmqpMessageWithNonAnonymousSender() {
        String topic = UUID.randomUUID().toString();
        AtomicInteger expected = new AtomicInteger(0);

        List<AmqpMessage<String>> messages = new ArrayList<>();
        usage.consume(topic,
                v -> {
                    expected.getAndIncrement();
                    v.getDelegate().accepted();
                    messages.add(new AmqpMessage<>(v, null, null));
                });

        SubscriberBuilder<? extends Message<?>, Void> sink = createProviderAndNonAnonymousSink(topic);

        //noinspection unchecked
        Multi.createFrom().range(0, 10)
                .map(v -> AmqpMessage.<String> builder()
                        .withBody(HELLO + v)
                        .withSubject("foo")
                        .withAddress("unused")
                        .build())
                .subscribe((Subscriber<? super AmqpMessage<String>>) sink.build());

        await().untilAtomic(expected, is(10));
        assertThat(expected).hasValue(10);

        messages.forEach(m -> {
            assertThat(m.getPayload()).isInstanceOf(String.class).startsWith(HELLO);
            assertThat(m.getSubject()).isEqualTo("foo");
        });
    }

    @Test
    public void testSinkUsingVertxAmqpMessage() {
        String topic = UUID.randomUUID().toString();
        AtomicInteger expected = new AtomicInteger(0);

        List<AmqpMessage<String>> messages = new CopyOnWriteArrayList<>();
        usage.consume(topic,
                v -> {
                    expected.getAndIncrement();
                    messages.add(new AmqpMessage<>(v, null, null));
                });

        SubscriberBuilder<? extends Message<?>, Void> sink = createProviderAndSink(topic);

        //noinspection unchecked
        Multi.createFrom().range(0, 10)
                .map(v -> io.vertx.mutiny.amqp.AmqpMessage.create()
                        .withBody(HELLO + v)
                        .subject("bar")
                        .build())
                .map(Message::of)
                .subscribe((Subscriber<? super Message<io.vertx.mutiny.amqp.AmqpMessage>>) sink.build());

        await().untilAtomic(expected, is(10));
        assertThat(expected).hasValue(10);

        messages.forEach(m -> {
            assertThat(m.getPayload()).isInstanceOf(String.class).startsWith(HELLO);
            assertThat(m.getSubject()).isEqualTo("bar");
        });
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testSinkUsingAmqpMessageAndChannelNameProperty() {
        String topic = UUID.randomUUID().toString();
        AtomicInteger expected = new AtomicInteger(0);

        List<AmqpMessage<String>> messages = new ArrayList<>();
        usage.consume(topic,
                v -> {
                    expected.getAndIncrement();
                    messages.add(new AmqpMessage<>(v, null, null));
                });

        SubscriberBuilder<? extends Message<?>, Void> sink = createProviderAndSinkUsingChannelName(topic);

        //noinspection unchecked
        Multi.createFrom().range(0, 10)
                .map(v -> AmqpMessage.<String> builder().withBody(HELLO + v).withSubject("foo").build())
                .subscribe((Subscriber<? super AmqpMessage<String>>) sink.build());

        await().untilAtomic(expected, is(10));
        assertThat(expected).hasValue(10);

        messages.forEach(m -> {
            assertThat(m.getPayload()).isInstanceOf(String.class).startsWith(HELLO);
            assertThat(m.getSubject()).isEqualTo("foo");
        });
    }

    @Test
    public void testConfigByCDIMissingBean() {
        Weld weld = new Weld();

        weld.addBeanClass(ProducingBean.class);

        new MapBasedConfig()
                .put("mp.messaging.outgoing.sink.address", "sink")
                .put("mp.messaging.outgoing.sink.connector", AmqpConnector.CONNECTOR_NAME)
                .put("mp.messaging.outgoing.sink.host", host)
                .put("mp.messaging.outgoing.sink.port", port)

                .put("amqp-username", username)
                .put("amqp-password", password)
                .put("mp.messaging.outgoing.sink.client-options-name", "myclientoptions")
                .write();

        assertThatThrownBy(() -> container = weld.initialize()).isInstanceOf(DeploymentException.class);
    }

    @Test
    public void testConfigByCDIIncorrectBean() {
        Weld weld = new Weld();

        weld.addBeanClass(ProducingBean.class);
        weld.addBeanClass(ClientConfigurationBean.class);

        new MapBasedConfig()
                .put("mp.messaging.outgoing.sink.address", "sink")
                .put("mp.messaging.outgoing.sink.connector", AmqpConnector.CONNECTOR_NAME)
                .put("mp.messaging.outgoing.sink.host", host)
                .put("mp.messaging.outgoing.sink.port", port)
                .put("amqp-username", username)
                .put("amqp-password", password)
                .put("mp.messaging.outgoing.sink.client-options-name", "dummyoptionsnonexistent")
                .write();

        assertThatThrownBy(() -> container = weld.initialize()).isInstanceOf(DeploymentException.class);
    }

    @Test
    public void testConfigByCDICorrect() throws InterruptedException {
        Weld weld = new Weld();

        CountDownLatch latch = new CountDownLatch(10);
        usage.consumeIntegers("sink",
                v -> latch.countDown());

        weld.addBeanClass(ProducingBean.class);
        weld.addBeanClass(ClientConfigurationBean.class);

        new MapBasedConfig()
                .put("mp.messaging.outgoing.sink.address", "sink")
                .put("mp.messaging.outgoing.sink.connector", AmqpConnector.CONNECTOR_NAME)
                .put("mp.messaging.outgoing.sink.host", host)
                .put("mp.messaging.outgoing.sink.port", port)
                .put("mp.messaging.outgoing.sink.durable", false)
                .put("amqp-username", username)
                .put("amqp-password", password)
                .put("mp.messaging.outgoing.sink.client-options-name", "myclientoptions")
                .write();

        container = weld.initialize();

        assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    @Disabled("Failing on CI - need to be investigated")
    public void testConfigGlobalOptionsByCDIMissingBean() {
        Weld weld = new Weld();

        weld.addBeanClass(ProducingBean.class);

        new MapBasedConfig()
                .put("mp.messaging.outgoing.sink.address", "sink")
                .put("mp.messaging.outgoing.sink.connector", AmqpConnector.CONNECTOR_NAME)
                .put("mp.messaging.outgoing.sink.host", host)
                .put("mp.messaging.outgoing.sink.port", port)
                .put("amqp-username", username)
                .put("amqp-password", password)
                .put("amqp-client-options-name", "dummyoptionsnonexistent")
                .write();

        assertThatThrownBy(() -> {
            container = weld.initialize();
        }).isInstanceOf(DeploymentException.class);
    }

    @Test
    @Disabled("Failing on CI - to be investigated")
    public void testConfigGlobalOptionsByCDIIncorrectBean() {
        Weld weld = new Weld();

        weld.addBeanClass(ProducingBean.class);
        weld.addBeanClass(ClientConfigurationBean.class);

        new MapBasedConfig()
                .put("mp.messaging.outgoing.sink.address", "sink")
                .put("mp.messaging.outgoing.sink.connector", AmqpConnector.CONNECTOR_NAME)
                .put("mp.messaging.outgoing.sink.host", host)
                .put("mp.messaging.outgoing.sink.port", port)
                .put("mp.messaging.outgoing.sink.durable", false)
                .put("amqp-username", username)
                .put("amqp-password", password)
                .put("amqp-client-options-name", "dummyoptionsnonexistent")
                .write();

        assertThatThrownBy(() -> {
            container = weld.initialize();
        }).isInstanceOf(DeploymentException.class);
    }

    @Test
    public void testConfigGlobalOptionsByCDICorrect() throws InterruptedException {
        Weld weld = new Weld();

        CountDownLatch latch = new CountDownLatch(10);
        usage.consumeIntegers("sink",
                v -> latch.countDown());

        weld.addBeanClass(ProducingBean.class);
        weld.addBeanClass(ClientConfigurationBean.class);

        new MapBasedConfig()
                .put("mp.messaging.outgoing.sink.address", "sink")
                .put("mp.messaging.outgoing.sink.connector", AmqpConnector.CONNECTOR_NAME)
                .put("mp.messaging.outgoing.sink.host", host)
                .put("mp.messaging.outgoing.sink.port", port)
                .put("mp.messaging.outgoing.sink.durable", false)
                .put("amqp-username", username)
                .put("amqp-password", password)
                .put("amqp-client-options-name", "myclientoptions")
                .write();

        container = weld.initialize();

        assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    public void testOutgoingMetadata() {
        String topic = UUID.randomUUID().toString();
        List<io.vertx.mutiny.amqp.AmqpMessage> messages = new CopyOnWriteArrayList<>();
        usage.consume(topic, messages::add);

        SubscriberBuilder<? extends Message<?>, Void> sink = createProviderAndSink(topic);
        //noinspection unchecked
        Multi.createFrom().range(0, 10)
                .map(Message::of)
                .map(m -> m.addMetadata(OutgoingAmqpMetadata.builder()
                        .withSubject("subject")
                        .withMessageId("my-id")
                        .withReplyTo("reply-to")
                        .withReplyToGroupId("group")
                        .withPriority((short) 4)
                        .withTtl(2000)
                        .withGroupId("group")
                        .withContentType("text/plain")
                        .withApplicationProperties(new JsonObject().put("key", "value"))
                        .withMessageAnnotations("some-annotation", "something important")
                        .withDeliveryAnnotations("some-delivery-annotation", "another important config")
                        .withCorrelationId("correlation-" + m.getPayload())
                        .withFooter("my-trailer", "hello-footer")
                        .build()))
                .subscribe((Subscriber<? super Message<Integer>>) sink.build());

        await().until(() -> messages.size() == 10);

        assertThat(messages).allSatisfy(msg -> {
            assertThat(msg.contentType()).isEqualTo("text/plain");
            assertThat(msg.subject()).isEqualTo("subject");
            assertThat(msg.getDelegate().unwrap().getMessageId()).isEqualTo("my-id");
            assertThat(msg.getDelegate().unwrap().getReplyToGroupId()).isEqualTo("group");
            assertThat(msg.replyTo()).isEqualTo("reply-to");
            assertThat(msg.priority()).isEqualTo((short) 4);
            assertThat(msg.correlationId()).startsWith("correlation-");
            assertThat(msg.groupId()).isEqualTo("group");
            assertThat(msg.ttl()).isEqualTo(2000);
            assertThat(msg.applicationProperties()).containsExactly(entry("key", "value"));
            assertThat(msg.getDelegate().unwrap().getMessageAnnotations().getValue())
                    .containsExactly(entry(Symbol.valueOf("some-annotation"), "something important"));
        });

        assertThat(messages).allSatisfy(msg -> {
            IncomingAmqpMetadata metadata = new IncomingAmqpMetadata(msg.getDelegate());
            assertThat(metadata.getContentType()).isEqualTo("text/plain");
            assertThat(metadata.getSubject()).isEqualTo("subject");
            assertThat(metadata.getId()).isEqualTo("my-id");
            assertThat(metadata.getReplyToGroupId()).isEqualTo("group");
            assertThat(metadata.getReplyTo()).isEqualTo("reply-to");
            assertThat(metadata.getPriority()).isEqualTo((short) 4);
            assertThat(metadata.getTtl()).isEqualTo(2000);
            assertThat(metadata.getCorrelationId()).startsWith("correlation-");
            assertThat(metadata.getGroupId()).isEqualTo("group");
            assertThat(metadata.getProperties()).containsExactly(entry("key", "value"));
            assertThat(metadata.getMessageAnnotations().getValue())
                    .containsExactly(entry(Symbol.valueOf("some-annotation"), "something important"));
            // Delivery annotations are not received.
            assertThat(metadata.getDeliveryAnnotations()).isNull();
            assertThat(metadata.isFirstAcquirer()).isFalse();
            //noinspection unchecked
            assertThat(metadata.getFooter().getValue()).containsExactly(entry("my-trailer", "hello-footer"));
            assertThat(metadata.getUserId()).isNull();
        });
    }

    @Test
    public void testOutgoingMetadataWithTtlSetOnConnector() {
        String topic = UUID.randomUUID().toString();
        List<io.vertx.mutiny.amqp.AmqpMessage> messages = new CopyOnWriteArrayList<>();
        usage.consume(topic, messages::add);

        SubscriberBuilder<? extends Message<?>, Void> sink = createProviderAndSink(topic, 3000);
        //noinspection unchecked
        Multi.createFrom().range(0, 10)
                .map(Message::of)
                .map(m -> m.addMetadata(OutgoingAmqpMetadata.builder()
                        .withSubject("subject")
                        .withMessageId("my-id")
                        .withReplyTo("reply-to")
                        .withReplyToGroupId("group")
                        .withPriority((short) 4)
                        .withGroupId("group")
                        .withContentType("text/plain")
                        .withMessageAnnotations("some-annotation", "something important")
                        .withDeliveryAnnotations("some-delivery-annotation", "another important config")
                        .withCorrelationId("correlation-" + m.getPayload())
                        .build()))
                .subscribe((Subscriber<? super Message<Integer>>) sink.build());

        await().until(() -> messages.size() == 10);

        assertThat(messages).allSatisfy(msg -> {
            assertThat(msg.contentType()).isEqualTo("text/plain");
            assertThat(msg.subject()).isEqualTo("subject");
            assertThat(msg.getDelegate().unwrap().getMessageId()).isEqualTo("my-id");
            assertThat(msg.getDelegate().unwrap().getReplyToGroupId()).isEqualTo("group");
            assertThat(msg.replyTo()).isEqualTo("reply-to");
            assertThat(msg.priority()).isEqualTo((short) 4);
            assertThat(msg.correlationId()).startsWith("correlation-");
            assertThat(msg.groupId()).isEqualTo("group");
            assertThat(msg.ttl()).isEqualTo(3000);
            assertThat(msg.applicationProperties()).isNull();
            assertThat(msg.getDelegate().unwrap().getMessageAnnotations().getValue())
                    .containsExactly(entry(Symbol.valueOf("some-annotation"), "something important"));
        });

        assertThat(messages).allSatisfy(msg -> {
            IncomingAmqpMetadata metadata = new IncomingAmqpMetadata(msg.getDelegate());
            assertThat(metadata.getContentType()).isEqualTo("text/plain");
            assertThat(metadata.getSubject()).isEqualTo("subject");
            assertThat(metadata.getId()).isEqualTo("my-id");
            assertThat(metadata.getReplyToGroupId()).isEqualTo("group");
            assertThat(metadata.getReplyTo()).isEqualTo("reply-to");
            assertThat(metadata.getPriority()).isEqualTo((short) 4);
            assertThat(metadata.getTtl()).isEqualTo(3000);
            assertThat(metadata.getCorrelationId()).startsWith("correlation-");
            assertThat(metadata.getGroupId()).isEqualTo("group");
            assertThat(metadata.getProperties()).isNull();
            assertThat(metadata.getMessageAnnotations().getValue())
                    .containsExactly(entry(Symbol.valueOf("some-annotation"), "something important"));
            // Delivery annotations are not received.
            assertThat(metadata.getDeliveryAnnotations()).isNull();
            assertThat(metadata.isFirstAcquirer()).isFalse();
            assertThat(metadata.getFooter()).isNull();
            assertThat(metadata.getUserId()).isNull();
        });
    }

    @Test
    public void testOutgoingMetadataWithTtlSetOnConnectorButOverriddenInMessage() {
        String topic = UUID.randomUUID().toString();
        List<io.vertx.mutiny.amqp.AmqpMessage> messages = new CopyOnWriteArrayList<>();
        usage.consume(topic, messages::add);

        SubscriberBuilder<? extends Message<?>, Void> sink = createProviderAndSink(topic, 3000);
        //noinspection unchecked
        Multi.createFrom().range(0, 10)
                .map(Message::of)
                .map(m -> m.addMetadata(OutgoingAmqpMetadata.builder()
                        .withSubject("subject")
                        .withMessageId("my-id")
                        .withReplyTo("reply-to")
                        .withReplyToGroupId("group")
                        .withPriority((short) 4)
                        .withTtl(4000)
                        .withGroupId("group")
                        .withContentType("text/plain")
                        .withApplicationProperties(new JsonObject().put("key", "value"))
                        .withMessageAnnotations("some-annotation", "something important")
                        .withDeliveryAnnotations("some-delivery-annotation", "another important config")
                        .withCorrelationId("correlation-" + m.getPayload())
                        .build()))
                .subscribe((Subscriber<? super Message<Integer>>) sink.build());

        await().until(() -> messages.size() == 10);

        assertThat(messages).allSatisfy(msg -> {
            assertThat(msg.contentType()).isEqualTo("text/plain");
            assertThat(msg.subject()).isEqualTo("subject");
            assertThat(msg.getDelegate().unwrap().getMessageId()).isEqualTo("my-id");
            assertThat(msg.getDelegate().unwrap().getReplyToGroupId()).isEqualTo("group");
            assertThat(msg.replyTo()).isEqualTo("reply-to");
            assertThat(msg.priority()).isEqualTo((short) 4);
            assertThat(msg.correlationId()).startsWith("correlation-");
            assertThat(msg.groupId()).isEqualTo("group");
            assertThat(msg.ttl()).isEqualTo(4000);
            assertThat(msg.applicationProperties()).containsExactly(entry("key", "value"));
            assertThat(msg.getDelegate().unwrap().getMessageAnnotations().getValue())
                    .containsExactly(entry(Symbol.valueOf("some-annotation"), "something important"));
        });

        assertThat(messages).allSatisfy(msg -> {
            IncomingAmqpMetadata metadata = new IncomingAmqpMetadata(msg.getDelegate());
            assertThat(metadata.getContentType()).isEqualTo("text/plain");
            assertThat(metadata.getSubject()).isEqualTo("subject");
            assertThat(metadata.getId()).isEqualTo("my-id");
            assertThat(metadata.getReplyToGroupId()).isEqualTo("group");
            assertThat(metadata.getReplyTo()).isEqualTo("reply-to");
            assertThat(metadata.getPriority()).isEqualTo((short) 4);
            assertThat(metadata.getTtl()).isEqualTo(4000);
            assertThat(metadata.getCorrelationId()).startsWith("correlation-");
            assertThat(metadata.getGroupId()).isEqualTo("group");
            assertThat(metadata.getProperties()).containsExactly(entry("key", "value"));
            assertThat(metadata.getMessageAnnotations().getValue())
                    .containsExactly(entry(Symbol.valueOf("some-annotation"), "something important"));
            // Delivery annotations are not received.
            assertThat(metadata.getDeliveryAnnotations()).isNull();
            assertThat(metadata.isFirstAcquirer()).isFalse();
            assertThat(metadata.getFooter()).isNull();
            assertThat(metadata.getUserId()).isNull();
        });
    }

    private SubscriberBuilder<? extends Message<?>, Void> createProviderAndSink(String topic) {
        Map<String, Object> config = new HashMap<>();
        config.put(ConnectorFactory.CHANNEL_NAME_ATTRIBUTE, topic);
        config.put("address", topic);
        config.put("name", "the name");
        config.put("host", host);
        config.put("durable", false);
        config.put("port", port);
        config.put("username", username);
        config.put("password", password);

        this.provider = new AmqpConnector();
        provider.setup(executionHolder);
        return this.provider.getSubscriberBuilder(new MapBasedConfig(config));
    }

    private SubscriberBuilder<? extends Message<?>, Void> createProviderAndSink(String topic, long ttl) {
        Map<String, Object> config = new HashMap<>();
        config.put(ConnectorFactory.CHANNEL_NAME_ATTRIBUTE, topic);
        config.put("address", topic);
        config.put("name", "the name");
        config.put("ttl", ttl);
        config.put("host", host);
        config.put("durable", false);
        config.put("port", port);
        config.put("username", username);
        config.put("password", password);

        this.provider = new AmqpConnector();
        provider.setup(executionHolder);
        return this.provider.getSubscriberBuilder(new MapBasedConfig(config));
    }

    private SubscriberBuilder<? extends Message<?>, Void> createProviderAndNonAnonymousSink(String topic) {
        Map<String, Object> config = new HashMap<>();
        config.put(ConnectorFactory.CHANNEL_NAME_ATTRIBUTE, topic);
        config.put("address", topic);
        config.put("name", "the name");
        config.put("host", host);
        config.put("durable", false);
        config.put("port", port);
        config.put("use-anonymous-sender", false);
        config.put("username", username);
        config.put("password", password);

        this.provider = new AmqpConnector();
        provider.setup(executionHolder);
        return this.provider.getSubscriberBuilder(new MapBasedConfig(config));
    }

    private SubscriberBuilder<? extends Message<?>, Void> createProviderAndSinkUsingChannelName(String topic) {
        Map<String, Object> config = new HashMap<>();
        config.put(ConnectorFactory.CHANNEL_NAME_ATTRIBUTE, topic);
        config.put("name", "the name");
        config.put("host", host);
        config.put("durable", false);
        config.put("port", port);
        config.put("username", username);
        config.put("password", password);

        this.provider = new AmqpConnector();
        provider.setup(executionHolder);
        return this.provider.getSubscriberBuilder(new MapBasedConfig(config));
    }

}
