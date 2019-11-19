package io.smallrye.reactive.messaging.amqp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.awaitility.Awaitility.await;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.LoggerFactory;

import io.smallrye.reactive.messaging.extension.MediatorManager;
import io.vertx.axle.amqp.AmqpMessage;
import io.vertx.axle.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import repeat.Repeat;

public class AmqpSourceTest extends AmqpTestBase {

    private AmqpConnector provider;

    private WeldContainer container;

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
    }

    @Test
    public void testSource() {
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = getConfig(topic);
        config.put("ttl", 10000);
        config.put("durable", false);

        provider = new AmqpConnector();
        provider.init();
        PublisherBuilder<? extends Message> builder = provider.getPublisherBuilder(new MapBasedConfig(config));

        List<Message> messages = new ArrayList<>();

        AtomicBoolean opened = new AtomicBoolean();
        builder.buildRs().subscribe(createSubscriber(messages, opened));
        await().until(opened::get);

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
        provider.init();
        PublisherBuilder<? extends Message> builder = provider.getPublisherBuilder(new MapBasedConfig(config));

        List<Message> messages = new ArrayList<>();

        AtomicBoolean opened = new AtomicBoolean();
        builder.buildRs().subscribe(createSubscriber(messages, opened));
        await().until(opened::get);

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
    private <T> Subscriber<T> createSubscriber(List<T> messages, AtomicBoolean opened) {
        //noinspection SubscriberImplementation - Seriously IntelliJ ????
        return new Subscriber<T>() {
            Subscription sub;

            @Override
            public void onSubscribe(Subscription s) {
                this.sub = s;
                sub.request(5);
                opened.set(true);
            }

            @Override
            public void onNext(T message) {
                messages.add(message);
                sub.request(1);
            }

            @Override
            public void onError(Throwable t) {
                LoggerFactory.getLogger("SUBSCRIBER").error("Error caught in stream", t);
            }

            @Override
            public void onComplete() {
                // Do nothing.
            }
        };
    }

    @Test
    @Repeat(times = 10)
    public void testBroadcast() {
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = new HashMap<>();
        config.put("address", topic);
        config.put("host", address);
        config.put("name", "the name for broadcast");
        config.put("port", port);
        config.put("broadcast", true);
        config.put("username", "artemis");
        config.put("password", new String("simetraehcapa".getBytes()));

        provider = new AmqpConnector();
        provider.init();
        PublisherBuilder<? extends Message> builder = provider.getPublisherBuilder(new MapBasedConfig(config));
        Publisher<? extends Message> rs = builder.buildRs();
        List<Message> messages1 = new ArrayList<>();
        List<Message> messages2 = new ArrayList<>();

        AtomicBoolean o1 = new AtomicBoolean();
        AtomicBoolean o2 = new AtomicBoolean();
        rs.subscribe(createSubscriber(messages1, o1));
        rs.subscribe(createSubscriber(messages2, o2));

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
    }

    @Test
    public void testABeanConsumingTheAMQPMessages() {
        System.setProperty("mp-config", "incoming");
        ConsumptionBean bean = deploy();
        List<Integer> list = bean.getResults();
        assertThat(list).isEmpty();

        AtomicInteger counter = new AtomicInteger();
        usage.produceTenIntegers("data", counter::getAndIncrement);

        await().atMost(2, TimeUnit.MINUTES).until(() -> list.size() >= 10);
        assertThat(list).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    }

    private ConsumptionBean deploy() {
        Weld weld = new Weld();
        weld.addBeanClass(AmqpConnector.class);
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
        provider.init();

        List<Message<byte[]>> messages = new ArrayList<>();
        PublisherBuilder<? extends Message> builder = provider.getPublisherBuilder(new MapBasedConfig(config));
        AtomicBoolean opened = new AtomicBoolean();

        //noinspection unchecked
        builder.to((Subscriber) createSubscriber(messages, opened)).run();
        await().until(opened::get);

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
        provider.init();

        List<Message<JsonObject>> messages = new ArrayList<>();
        PublisherBuilder<? extends Message> builder = provider.getPublisherBuilder(new MapBasedConfig(config));
        AtomicBoolean opened = new AtomicBoolean();

        //noinspection unchecked
        builder.to((Subscriber) createSubscriber(messages, opened)).run();
        await().until(opened::get);

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
        provider.init();

        List<Message<JsonArray>> messages = new ArrayList<>();
        PublisherBuilder<? extends Message> builder = provider.getPublisherBuilder(new MapBasedConfig(config));
        AtomicBoolean opened = new AtomicBoolean();

        //noinspection unchecked
        builder.to((Subscriber) createSubscriber(messages, opened)).run();
        await().until(opened::get);

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
        provider.init();

        PublisherBuilder<? extends Message> builder = provider.getPublisherBuilder(new MapBasedConfig(config));
        AtomicBoolean opened = new AtomicBoolean();

        //noinspection unchecked
        builder.to((Subscriber) createSubscriber(messages, opened)).run();
        await().until(opened::get);

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
        provider.init();

        PublisherBuilder<? extends Message> builder = provider.getPublisherBuilder(new MapBasedConfig(config));
        AtomicBoolean opened = new AtomicBoolean();

        //noinspection unchecked
        builder.to((Subscriber) createSubscriber(messages, opened)).run();
        await().until(opened::get);

        List<String> list = new ArrayList<>();
        list.add("hello");
        list.add("world");
        usage.produce(topic, 1, () -> new Data(new Binary(list.toString().getBytes())));

        await().atMost(2, TimeUnit.MINUTES).until(() -> !messages.isEmpty());
        byte[] result = messages.get(0).getPayload();
        assertThat(new String(result))
                .isEqualTo(list.toString());
    }

    @NotNull
    private Map<String, Object> getConfig(String topic) {
        Map<String, Object> config = new HashMap<>();
        config.put("address", topic);
        config.put("host", address);
        config.put("port", port);
        config.put("name", "some name");
        config.put("username", "artemis");
        config.put("password", new String("simetraehcapa".getBytes()));
        return config;
    }

    @NotNull
    private Map<String, Object> getConfigUsingChannelName(String topic) {
        Map<String, Object> config = new HashMap<>();
        config.put("channel-name", topic);
        config.put("host", address);
        config.put("port", port);
        config.put("name", "some name");
        config.put("username", "artemis");
        config.put("password", new String("simetraehcapa".getBytes()));
        return config;
    }

}
