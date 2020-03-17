package io.smallrye.reactive.messaging.amqp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.core.Is.is;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.jboss.weld.exceptions.DeploymentException;
import org.junit.After;
import org.junit.Test;
import org.reactivestreams.Subscriber;

import io.smallrye.mutiny.Multi;
import repeat.Repeat;

public class AmqpSinkTest extends AmqpTestBase {

    private static final String HELLO = "hello-";
    private WeldContainer container;
    private AmqpConnector provider;

    @After
    public void cleanup() {
        if (provider != null) {
            provider.close();
            provider.terminate(null);
        }

        if (container != null) {
            container.shutdown();
        }

        System.clearProperty("mp-config");
        System.clearProperty("client-options-name");
        System.clearProperty("amqp-client-options-name");
    }

    @Test
    public void testSinkUsingInteger() {
        String topic = UUID.randomUUID().toString();
        AtomicInteger expected = new AtomicInteger(0);
        usage.consumeIntegers(topic,
                v -> expected.getAndIncrement());

        SubscriberBuilder<? extends Message, Void> sink = createProviderAndSink(topic);
        //noinspection unchecked
        Multi.createFrom().range(0, 10)
                .map(v -> (Message) Message.of(v))
                .subscribe((Subscriber) sink.build());

        await().until(() -> expected.get() == 10);
        assertThat(expected).hasValue(10);
    }

    @Test
    public void testSinkUsingString() {
        String topic = UUID.randomUUID().toString();

        SubscriberBuilder<? extends Message, Void> sink = createProviderAndSink(topic);

        AtomicInteger expected = new AtomicInteger(0);
        usage.consumeStrings(topic,
                v -> expected.getAndIncrement());

        //noinspection unchecked
        Multi.createFrom().range(0, 10)
                .map(i -> Integer.toString(i))
                .map(Message::of)
                .subscribe((Subscriber) sink.build());

        await().untilAtomic(expected, is(10));
        assertThat(expected).hasValue(10);
    }

    @Test
    @Repeat(times = 3)
    public void testABeanProducingMessagesSentToAMQP() throws InterruptedException {
        Weld weld = new Weld();

        CountDownLatch latch = new CountDownLatch(10);
        usage.consumeIntegers("sink",
                v -> latch.countDown());

        weld.addBeanClass(AmqpConnector.class);
        weld.addBeanClass(ProducingBean.class);

        System.setProperty("mp-config", "outgoing");

        container = weld.initialize();

        assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    public void testSinkUsingAmqpMessage() {
        String topic = UUID.randomUUID().toString();
        AtomicInteger expected = new AtomicInteger(0);

        List<AmqpMessage<String>> messages = new ArrayList<>();
        usage.consume(topic,
                v -> {
                    expected.getAndIncrement();
                    v.getDelegate().accepted();
                    messages.add(new AmqpMessage<>(v));
                });

        SubscriberBuilder<? extends Message, Void> sink = createProviderAndSink(topic);

        //noinspection unchecked
        Multi.createFrom().range(0, 10)
                .map(v -> {
                    AmqpMessage<String> message = AmqpMessage.<String> builder()
                            .withBody(HELLO + v)
                            .withSubject("foo")
                            .build();
                    return message;
                })
                .subscribe((Subscriber) sink.build());

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
        usage.<String> consume(topic,
                v -> {
                    expected.getAndIncrement();
                    messages.add(new AmqpMessage<>(v));
                });

        SubscriberBuilder<? extends Message, Void> sink = createProviderAndSink(topic);

        //noinspection unchecked
        Multi.createFrom().range(0, 10)
                .map(v -> io.vertx.mutiny.amqp.AmqpMessage.create()
                        .withBody(HELLO + v)
                        .subject("bar")
                        .build())
                .map(Message::of)
                .subscribe((Subscriber) sink.build());

        await().untilAtomic(expected, is(10));
        assertThat(expected).hasValue(10);

        messages.forEach(m -> {
            assertThat(m.getPayload()).isInstanceOf(String.class).startsWith(HELLO);
            assertThat(m.getSubject()).isEqualTo("bar");
        });
    }

    @Test
    public void testSinkUsingAmqpMessageAndChannelNameProperty() {
        String topic = UUID.randomUUID().toString();
        AtomicInteger expected = new AtomicInteger(0);

        List<AmqpMessage<String>> messages = new ArrayList<>();
        usage.<String> consume(topic,
                v -> {
                    expected.getAndIncrement();
                    messages.add(new AmqpMessage<>(v));
                });

        SubscriberBuilder<? extends Message, Void> sink = createProviderAndSinkUsingChannelName(topic);

        //noinspection unchecked
        Multi.createFrom().range(0, 10)
                .map(v -> AmqpMessage.<String> builder().withBody(HELLO + v).withSubject("foo").build())
                .subscribe((Subscriber) sink.build());

        await().untilAtomic(expected, is(10));
        assertThat(expected).hasValue(10);

        messages.forEach(m -> {
            assertThat(m.getPayload()).isInstanceOf(String.class).startsWith(HELLO);
            assertThat(m.getSubject()).isEqualTo("foo");
        });
    }

    @Test(expected = DeploymentException.class)
    public void testConfigByCDIMissingBean() {
        Weld weld = new Weld();

        weld.addBeanClass(AmqpConnector.class);
        weld.addBeanClass(ProducingBean.class);

        System.setProperty("mp-config", "outgoing");
        System.setProperty("client-options-name", "myclientoptions");

        container = weld.initialize();
    }

    @Test(expected = DeploymentException.class)
    public void testConfigByCDIIncorrectBean() {
        Weld weld = new Weld();

        weld.addBeanClass(AmqpConnector.class);
        weld.addBeanClass(ProducingBean.class);
        weld.addBeanClass(ClientConfigurationBean.class);

        System.setProperty("mp-config", "outgoing");
        System.setProperty("client-options-name", "dummyoptionsnonexistent");

        container = weld.initialize();
    }

    @Test
    public void testConfigByCDICorrect() throws InterruptedException {
        Weld weld = new Weld();

        CountDownLatch latch = new CountDownLatch(10);
        usage.consumeIntegers("sink",
                v -> latch.countDown());

        weld.addBeanClass(AmqpConnector.class);
        weld.addBeanClass(ProducingBean.class);
        weld.addBeanClass(ClientConfigurationBean.class);

        System.setProperty("mp-config", "outgoing");
        System.setProperty("client-options-name", "myclientoptions");

        container = weld.initialize();

        assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test(expected = DeploymentException.class)
    public void testConfigGlobalOptionsByCDIMissingBean() {
        Weld weld = new Weld();

        weld.addBeanClass(AmqpConnector.class);
        weld.addBeanClass(ProducingBean.class);

        System.setProperty("mp-config", "outgoing");
        System.setProperty("amqp-client-options-name", "dummyoptionsnonexistent");

        container = weld.initialize();
    }

    @Test(expected = DeploymentException.class)
    public void testConfigGlobalOptionsByCDIIncorrectBean() {
        Weld weld = new Weld();

        weld.addBeanClass(AmqpConnector.class);
        weld.addBeanClass(ProducingBean.class);
        weld.addBeanClass(ClientConfigurationBean.class);

        System.setProperty("mp-config", "outgoing");
        System.setProperty("amqp-client-options-name", "dummyoptionsnonexistent");

        container = weld.initialize();
    }

    @Test
    public void testConfigGlobalOptionsByCDICorrect() throws InterruptedException {
        Weld weld = new Weld();

        CountDownLatch latch = new CountDownLatch(10);
        usage.consumeIntegers("sink",
                v -> latch.countDown());

        weld.addBeanClass(AmqpConnector.class);
        weld.addBeanClass(ProducingBean.class);
        weld.addBeanClass(ClientConfigurationBean.class);

        System.setProperty("mp-config", "outgoing");
        System.setProperty("amqp-client-options-name", "myclientoptions");

        container = weld.initialize();

        assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
    }

    private SubscriberBuilder<? extends Message, Void> createProviderAndSink(String topic) {
        Map<String, Object> config = new HashMap<>();
        config.put("address", topic);
        config.put("name", "the name");
        config.put("host", address);
        config.put("durable", false);
        config.put("port", port);
        config.put("username", "artemis");
        config.put("password", new String("simetraehcapa".getBytes()));

        this.provider = new AmqpConnector();
        provider.init();
        return this.provider.getSubscriberBuilder(new MapBasedConfig(config));
    }

    private SubscriberBuilder<? extends Message, Void> createProviderAndSinkUsingChannelName(String topic) {
        Map<String, Object> config = new HashMap<>();
        config.put("channel-name", topic);
        config.put("name", "the name");
        config.put("host", address);
        config.put("durable", false);
        config.put("port", port);
        config.put("username", "artemis");
        config.put("password", new String("simetraehcapa".getBytes()));

        this.provider = new AmqpConnector();
        provider.init();
        return this.provider.getSubscriberBuilder(new MapBasedConfig(config));
    }

}
