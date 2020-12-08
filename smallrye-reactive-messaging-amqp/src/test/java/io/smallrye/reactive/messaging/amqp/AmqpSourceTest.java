package io.smallrye.reactive.messaging.amqp;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static org.eclipse.microprofile.reactive.messaging.spi.ConnectorFactory.CHANNEL_NAME_ATTRIBUTE;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.jboss.logging.Logger;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.jboss.weld.exceptions.DeploymentException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.smallrye.common.constraint.NotNull;
import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.reactive.messaging.connectors.ExecutionHolder;
import io.smallrye.reactive.messaging.extension.MediatorManager;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.amqp.AmqpMessage;
import io.vertx.mutiny.core.buffer.Buffer;

public class AmqpSourceTest extends AmqpBrokerTestBase {

    private AmqpConnector provider;

    private WeldContainer container;

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

        System.clearProperty("mp-config");
        System.clearProperty("client-options-name");
        System.clearProperty("amqp-client-options-name");
    }

    @Test
    public void testSource() {
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = getConfig(topic);
        config.put("ttl", 10000);
        config.put("durable", false);

        provider = new AmqpConnector();
        provider.setup(executionHolder);
        PublisherBuilder<? extends Message<?>> builder = provider.getPublisherBuilder(new MapBasedConfig(config));

        List<Message<Integer>> messages = new ArrayList<>();

        AtomicBoolean opened = new AtomicBoolean();
        builder.buildRs().subscribe(createSubscriber(messages, opened));
        await().until(opened::get);

        await().until(() -> isAmqpConnectorReady(provider));
        await().until(() -> isAmqpConnectorAlive(provider));

        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceTenIntegers(topic,
                counter::getAndIncrement)).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages.size() >= 10);
        assertThat(messages.stream()
                .peek(m -> m.ack().toCompletableFuture().join())
                .map(Message::getPayload)
                .collect(Collectors.toList()))
                        .containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    }

    @Test
    public void testSourceUsingChannelName() {
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = getConfigUsingChannelName(topic);
        config.put("ttl", 10000);
        config.put("durable", false);

        provider = new AmqpConnector();
        provider.setup(executionHolder);
        PublisherBuilder<? extends Message<?>> builder = provider.getPublisherBuilder(new MapBasedConfig(config));

        List<Message<Integer>> messages = new ArrayList<>();

        AtomicBoolean opened = new AtomicBoolean();
        builder.buildRs().subscribe(createSubscriber(messages, opened));
        await().until(opened::get);

        await().until(() -> isAmqpConnectorReady(provider));
        await().until(() -> isAmqpConnectorAlive(provider));

        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceTenIntegers(topic,
                counter::getAndIncrement)).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages.size() >= 10);
        assertThat(messages.stream()
                .peek(m -> m.ack().toCompletableFuture().join())
                .map(Message::getPayload)
                .collect(Collectors.toList()))
                        .containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    }

    @NotNull
    private <T, O> Subscriber<T> createSubscriber(List<Message<O>> messages, AtomicBoolean opened) {
        //noinspection ReactiveStreamsSubscriberImplementation
        return new Subscriber<T>() {
            Subscription sub;

            @Override
            public void onSubscribe(Subscription s) {
                this.sub = s;
                sub.request(5);
                opened.set(true);
            }

            @SuppressWarnings("unchecked")
            @Override
            public void onNext(T message) {
                messages.add((Message<O>) message);
                sub.request(1);
            }

            @Override
            public void onError(Throwable t) {
                Logger.getLogger("SUBSCRIBER").error("Error caught in stream", t);
            }

            @Override
            public void onComplete() {
                // Do nothing.
            }
        };
    }

    @RepeatedTest(10)
    public void testBroadcast() {
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = new HashMap<>();
        config.put("address", topic);
        config.put(CHANNEL_NAME_ATTRIBUTE, topic);
        config.put("host", host);
        config.put("name", "the name for broadcast");
        config.put("port", port);
        config.put("broadcast", true);
        config.put("username", username);
        config.put("password", password);

        provider = new AmqpConnector();
        provider.setup(executionHolder);
        PublisherBuilder<? extends Message<?>> builder = provider.getPublisherBuilder(new MapBasedConfig(config));
        Publisher<? extends Message<?>> rs = builder.buildRs();
        List<Message<Integer>> messages1 = new ArrayList<>();
        List<Message<Integer>> messages2 = new ArrayList<>();

        AtomicBoolean o1 = new AtomicBoolean();
        AtomicBoolean o2 = new AtomicBoolean();
        rs.subscribe(createSubscriber(messages1, o1));
        rs.subscribe(createSubscriber(messages2, o2));

        await().until(() -> isAmqpConnectorReady(provider));

        await()
                .pollDelay(5, TimeUnit.SECONDS)
                .until(() -> o1.get() && o2.get());

        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceTenIntegers(topic,
                counter::getAndIncrement)).start();

        await().atMost(1, TimeUnit.MINUTES).until(() -> messages1.size() >= 10);
        await().atMost(1, TimeUnit.MINUTES).until(() -> messages2.size() >= 10);
        assertThat(messages1.stream().map(Message::getPayload)
                .collect(Collectors.toList()))
                        .containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        assertThat(messages2.stream().map(Message::getPayload)
                .collect(Collectors.toList()))
                        .containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        MapBasedConfig.clear();
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
    }

    @Test
    public void testABeanConsumingTheAMQPMessages() {
        new MapBasedConfig()
                .put("mp.messaging.incoming.data.address", "data")
                .put("mp.messaging.incoming.data.connector", AmqpConnector.CONNECTOR_NAME)
                .put("mp.messaging.incoming.data.host", host)
                .put("mp.messaging.incoming.data.port", port)
                .put("amqp-username", username)
                .put("amqp-password", password)
                .write();

        ConsumptionBean bean = deploy();

        await().until(() -> isAmqpConnectorReady(container));
        await().until(() -> isAmqpConnectorAlive(container));

        List<Integer> list = bean.getResults();
        assertThat(list).isEmpty();

        AtomicInteger counter = new AtomicInteger();
        usage.produceTenIntegers("data", counter::getAndIncrement);

        await().atMost(2, TimeUnit.MINUTES).until(() -> list.size() >= 10);
        assertThat(list).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    }

    private ConsumptionBean deploy() {
        Weld weld = new Weld();
        weld.addBeanClass(ConsumptionBean.class);

        container = weld.initialize();
        await().until(() -> container.select(MediatorManager.class).get().isInitialized());
        return container.getBeanManager().createInstance().select(ConsumptionBean.class).get();
    }

    @Test
    public void testSourceWithBinaryContent() {
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = getConfig(topic);
        provider = new AmqpConnector();
        provider.setup(executionHolder);

        List<Message<byte[]>> messages = new ArrayList<>();
        PublisherBuilder<? extends Message<?>> builder = provider.getPublisherBuilder(new MapBasedConfig(config));
        AtomicBoolean opened = new AtomicBoolean();

        builder.to(createSubscriber(messages, opened)).run();
        await().until(opened::get);

        await().until(() -> isAmqpConnectorReady(provider));
        await().until(() -> isAmqpConnectorAlive(provider));

        usage.produce(topic, 1, () -> AmqpMessage.create().withBufferAsBody(Buffer.buffer("foo".getBytes())).build());

        await().atMost(2, TimeUnit.MINUTES).until(() -> !messages.isEmpty());
        assertThat(messages.stream().map(Message::getPayload)
                .collect(Collectors.toList()))
                        .containsExactly("foo".getBytes());
    }

    @Test
    public void testSourceWithJsonObjectContent() {
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = getConfig(topic);
        provider = new AmqpConnector();
        provider.setup(executionHolder);

        List<Message<JsonObject>> messages = new ArrayList<>();
        PublisherBuilder<? extends Message<?>> builder = provider.getPublisherBuilder(new MapBasedConfig(config));
        AtomicBoolean opened = new AtomicBoolean();

        builder.to(createSubscriber(messages, opened)).run();
        await().until(opened::get);

        await().until(() -> isAmqpConnectorReady(provider));
        await().until(() -> isAmqpConnectorAlive(provider));

        JsonObject json = new JsonObject();
        String id = UUID.randomUUID().toString();
        json.put("key", id);
        json.put("some", "content");
        usage.produce(topic, 1, () -> AmqpMessage.create().withJsonObjectAsBody(json).build());

        await().atMost(2, TimeUnit.MINUTES).until(() -> !messages.isEmpty());
        JsonObject result = messages.get(0).getPayload();
        assertThat(result)
                .containsOnly(entry("key", id), entry("some", "content"));
    }

    @Test
    public void testSourceWithListContent() {
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = getConfig(topic);
        provider = new AmqpConnector();
        provider.setup(executionHolder);

        List<Message<JsonArray>> messages = new ArrayList<>();
        PublisherBuilder<? extends Message<?>> builder = provider.getPublisherBuilder(new MapBasedConfig(config));
        AtomicBoolean opened = new AtomicBoolean();

        builder.to(createSubscriber(messages, opened)).run();
        await().until(opened::get);

        await().until(() -> isAmqpConnectorReady(provider));
        await().until(() -> isAmqpConnectorAlive(provider));

        JsonArray list = new JsonArray();
        String id = UUID.randomUUID().toString();
        list.add("ola");
        list.add(id);
        usage.produce(topic, 1, () -> AmqpMessage.create().withJsonArrayAsBody(list).build());

        await().atMost(2, TimeUnit.MINUTES).until(() -> !messages.isEmpty());
        JsonArray result = messages.get(0).getPayload();
        assertThat(result)
                .containsExactly("ola", id);
    }

    @Test
    public void testSourceWithSeqContent() {
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = getConfig(topic);
        List<Message<List<String>>> messages = new ArrayList<>();
        provider = new AmqpConnector();
        provider.setup(executionHolder);

        PublisherBuilder<? extends Message<?>> builder = provider.getPublisherBuilder(new MapBasedConfig(config));
        AtomicBoolean opened = new AtomicBoolean();

        builder.to(createSubscriber(messages, opened)).run();
        await().until(opened::get);

        await().until(() -> isAmqpConnectorReady(provider));
        await().until(() -> isAmqpConnectorAlive(provider));

        List<String> list = new ArrayList<>();
        list.add("tag");
        list.add("bonjour");
        usage.produce(topic, 1, () -> new AmqpSequence(list));

        await().atMost(2, TimeUnit.MINUTES).until(() -> !messages.isEmpty());
        List<String> result = messages.get(0).getPayload();
        assertThat(result)
                .containsOnly("tag", "bonjour");
    }

    @Test
    public void testSourceWithDataContent() {
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = getConfig(topic);
        List<Message<byte[]>> messages = new ArrayList<>();
        provider = new AmqpConnector();
        provider.setup(executionHolder);

        PublisherBuilder<? extends Message<?>> builder = provider.getPublisherBuilder(new MapBasedConfig(config));
        AtomicBoolean opened = new AtomicBoolean();

        builder.to(createSubscriber(messages, opened)).run();
        await().until(opened::get);

        await().until(() -> isAmqpConnectorReady(provider));
        await().until(() -> isAmqpConnectorAlive(provider));

        List<String> list = new ArrayList<>();
        list.add("hello");
        list.add("world");
        usage.produce(topic, 1, () -> new Data(new Binary(list.toString().getBytes())));

        await().atMost(2, TimeUnit.MINUTES).until(() -> !messages.isEmpty());
        byte[] result = messages.get(0).getPayload();
        assertThat(new String(result))
                .isEqualTo(list.toString());
    }

    @Test
    public void testConfigByCDIMissingBean() {
        Weld weld = new Weld();

        weld.addBeanClass(ConsumptionBean.class);
        weld.addBeanClass(ExecutionHolder.class);

        new MapBasedConfig()
                .put("mp.messaging.incoming.data.address", "data")
                .put("mp.messaging.incoming.data.connector", AmqpConnector.CONNECTOR_NAME)
                .put("mp.messaging.incoming.data.host", host)
                .put("mp.messaging.incoming.data.port", port)
                .put("amqp-username", username)
                .put("amqp-password", password)
                .put("mp.messaging.incoming.data.client-options-name", "myclientoptions")
                .write();

        assertThatThrownBy(() -> container = weld.initialize())
                .isInstanceOf(DeploymentException.class);
    }

    @Test
    public void testConfigByCDIIncorrectBean() {
        Weld weld = new Weld();

        weld.addBeanClass(ConsumptionBean.class);
        weld.addBeanClass(ClientConfigurationBean.class);
        weld.addBeanClass(ExecutionHolder.class);

        new MapBasedConfig()
                .put("mp.messaging.incoming.data.address", "data")
                .put("mp.messaging.incoming.data.connector", AmqpConnector.CONNECTOR_NAME)
                .put("mp.messaging.incoming.data.host", host)
                .put("mp.messaging.incoming.data.port", port)
                .put("amqp-username", username)
                .put("amqp-password", password)
                .put("mp.messaging.incoming.data.client-options-name", "dummyoptionsnonexistent")
                .write();

        assertThatThrownBy(() -> container = weld.initialize())
                .isInstanceOf(DeploymentException.class);
    }

    @Test
    public void testConfigByCDICorrect() {
        Weld weld = new Weld();

        weld.addBeanClass(ClientConfigurationBean.class);
        weld.addBeanClass(ConsumptionBean.class);

        new MapBasedConfig()
                .put("mp.messaging.incoming.data.address", "data")
                .put("mp.messaging.incoming.data.connector", AmqpConnector.CONNECTOR_NAME)
                .put("mp.messaging.incoming.data.host", host)
                .put("mp.messaging.incoming.data.port", port)
                .put("amqp-username", username)
                .put("amqp-password", password)
                .put("mp.messaging.incoming.data.client-options-name", "myclientoptions")
                .write();

        container = weld.initialize();
        await().until(() -> container.select(MediatorManager.class).get().isInitialized());
        List<Integer> list = container.select(ConsumptionBean.class).get().getResults();
        assertThat(list).isEmpty();

        AtomicInteger counter = new AtomicInteger();
        usage.produceTenIntegers("data", counter::getAndIncrement);

        await().atMost(2, TimeUnit.MINUTES).until(() -> list.size() >= 10);
        assertThat(list).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    }

    @Test
    public void testConfigGlobalOptionsByCDICorrect() {
        Weld weld = new Weld();

        weld.addBeanClass(ClientConfigurationBean.class);
        weld.addBeanClass(ConsumptionBean.class);

        new MapBasedConfig()
                .put("mp.messaging.incoming.data.address", "data")
                .put("mp.messaging.incoming.data.connector", AmqpConnector.CONNECTOR_NAME)
                .put("mp.messaging.incoming.data.host", host)
                .put("mp.messaging.incoming.data.port", port)
                .put("amqp-username", username)
                .put("amqp-password", password)
                .put("amqp-client-options-name", "myclientoptions")
                .write();

        container = weld.initialize();
        await().until(() -> container.select(MediatorManager.class).get().isInitialized());
        List<Integer> list = container.select(ConsumptionBean.class).get().getResults();
        assertThat(list).isEmpty();

        AtomicInteger counter = new AtomicInteger();
        usage.produceTenIntegers("data", counter::getAndIncrement);

        await().atMost(2, TimeUnit.MINUTES).until(() -> list.size() >= 10);
        assertThat(list).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

    }

    @Test
    public void testConfigGlobalOptionsByCDIMissingBean() {
        Weld weld = new Weld();

        weld.addBeanClass(ConsumptionBean.class);
        weld.addBeanClass(ExecutionHolder.class);

        new MapBasedConfig()
                .put("mp.messaging.incoming.data.address", "data")
                .put("mp.messaging.incoming.data.connector", AmqpConnector.CONNECTOR_NAME)
                .put("mp.messaging.incoming.data.host", host)
                .put("mp.messaging.incoming.data.port", port)
                .put("amqp-username", username)
                .put("amqp-password", password)
                .put("amqp-client-options-name", "myclientoptions")
                .write();

        assertThatThrownBy(() -> container = weld.initialize())
                .isInstanceOf(DeploymentException.class);
    }

    @Test
    public void testConfigGlobalOptionsByCDIIncorrectBean() {
        Weld weld = new Weld();

        weld.addBeanClass(ConsumptionBean.class);
        weld.addBeanClass(ClientConfigurationBean.class);
        weld.addBeanClass(ExecutionHolder.class);

        new MapBasedConfig()
                .put("mp.messaging.incoming.data.address", "data")
                .put("mp.messaging.incoming.data.connector", AmqpConnector.CONNECTOR_NAME)
                .put("mp.messaging.incoming.data.host", host)
                .put("mp.messaging.incoming.data.port", port)
                .put("amqp-username", username)
                .put("amqp-password", password)
                .put("amqp-client-options-name", "dummyoptionsnonexistent")
                .write();

        assertThatThrownBy(() -> container = weld.initialize())
                .isInstanceOf(DeploymentException.class);
    }

    @NotNull
    private Map<String, Object> getConfig(String topic) {
        Map<String, Object> config = new HashMap<>();
        config.put("address", topic);
        config.put(CHANNEL_NAME_ATTRIBUTE, UUID.randomUUID().toString());
        config.put("host", host);
        config.put("port", port);
        config.put("name", "some name");
        config.put("username", username);
        config.put("password", password);
        return config;
    }

    @NotNull
    private Map<String, Object> getConfigUsingChannelName(String topic) {
        Map<String, Object> config = new HashMap<>();
        config.put(CHANNEL_NAME_ATTRIBUTE, topic);
        config.put("host", host);
        config.put("port", port);
        config.put("name", "some name");
        config.put("username", username);
        config.put("password", password);
        return config;
    }

}
