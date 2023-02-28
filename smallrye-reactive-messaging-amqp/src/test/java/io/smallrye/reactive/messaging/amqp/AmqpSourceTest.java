package io.smallrye.reactive.messaging.amqp;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static org.eclipse.microprofile.reactive.messaging.spi.ConnectorFactory.CHANNEL_NAME_ATTRIBUTE;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.transport.DeliveryState;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.logging.Logger;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import io.smallrye.common.constraint.NotNull;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class AmqpSourceTest extends AmqpTestBase {

    private final static Logger LOGGER = Logger.getLogger(AmqpSourceTest.class);

    private WeldContainer container;
    private AmqpConnector provider;
    private MockServer server;

    @AfterEach
    public void cleanup() {
        if (provider != null) {
            provider.terminate(null);
        }

        if (container != null) {
            container.shutdown();
        }

        if (server != null) {
            server.close();
        }
    }

    @Test
    @Timeout(30)
    public void testSource() throws Exception {
        doSourceTestImpl(false);
    }

    @Test
    @Timeout(30)
    public void testSourceUsingChannelName() throws Exception {
        doSourceTestImpl(true);
    }

    private void doSourceTestImpl(boolean useChannelName) throws Exception {
        int msgCount = 10;
        List<DispositionRecord> dispositionsReceived = Collections.synchronizedList(new ArrayList<>(msgCount));

        server = setupMockServer(msgCount, dispositionsReceived, executionHolder.vertx().getDelegate());

        String topic = UUID.randomUUID().toString();

        Map<String, Object> config;
        if (useChannelName) {
            config = getConfigUsingChannelName(topic, server.actualPort());
        } else {
            config = getConfig(topic, server.actualPort());
        }

        provider = new AmqpConnector();
        provider.setup(executionHolder);
        Flow.Publisher<? extends Message<?>> publisher = provider.getPublisher(new MapBasedConfig(config));

        List<Message<Integer>> messages = new ArrayList<>();

        publisher.subscribe(createSubscriber(messages, new AtomicBoolean()));

        await().atMost(5, TimeUnit.SECONDS).until(() -> messages.size() >= 10);
        assertThat(messages.stream()
                .peek(m -> m.ack().toCompletableFuture().join())
                .map(Message::getPayload)
                .collect(Collectors.toList()))
                .containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        await().atMost(2, TimeUnit.SECONDS).until(() -> dispositionsReceived.size() >= msgCount);

        AtomicInteger count = new AtomicInteger();
        dispositionsReceived.forEach(record -> {
            int messageNum = count.get() + 1;
            assertThat(messageNum).isLessThanOrEqualTo(msgCount);

            assertThat(record.getMessageNumber()).isEqualTo(messageNum);
            assertThat(record.getState()).isInstanceOf(Accepted.class);
            assertThat(record.isSettled()).isTrue();

            count.incrementAndGet();
        });

        assertThat(count.get()).isEqualTo(msgCount);
    }

    @NotNull
    private <T, O> Subscriber<T> createSubscriber(List<Message<O>> messages, AtomicBoolean opened) {
        //noinspection ReactiveStreamsSubscriberImplementation
        return new Subscriber<T>() {
            Flow.Subscription sub;

            @Override
            public void onSubscribe(Flow.Subscription s) {
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

    @Test
    @Timeout(30)
    public void testBroadcast() throws Exception {
        int msgCount = 10;
        CountDownLatch sendStartLatch = new CountDownLatch(1);
        List<DispositionRecord> dispositionsReceived = Collections.synchronizedList(new ArrayList<>(msgCount));

        server = setupMockServer(msgCount, dispositionsReceived, executionHolder.vertx().getDelegate(), sendStartLatch);

        String topic = "broadcast-" + UUID.randomUUID().toString();
        Map<String, Object> config = new HashMap<>();
        config.put("address", topic);
        config.put(CHANNEL_NAME_ATTRIBUTE, topic);
        config.put("host", "localhost");
        config.put("name", "the name for broadcast");
        config.put("port", server.actualPort());
        config.put("broadcast", true);
        config.put("tracing-enabled", false);

        provider = new AmqpConnector();
        provider.setup(executionHolder);
        Flow.Publisher<? extends Message<?>> publisher = provider.getPublisher(new MapBasedConfig(config));

        List<Message<Integer>> messages1 = new ArrayList<>();
        List<Message<Integer>> messages2 = new ArrayList<>();
        AtomicBoolean o1 = new AtomicBoolean();
        AtomicBoolean o2 = new AtomicBoolean();

        publisher.subscribe(createSubscriber(messages1, o1));
        publisher.subscribe(createSubscriber(messages2, o2));

        await().until(() -> o1.get() && o2.get());
        sendStartLatch.countDown();

        await().until(() -> isAmqpConnectorReady(provider));

        await().atMost(5, TimeUnit.SECONDS).until(() -> messages1.size() >= 10);
        await().atMost(5, TimeUnit.SECONDS).until(() -> messages2.size() >= 10);
        assertThat(messages1.stream().peek(m -> m.ack().toCompletableFuture().join())
                .map(Message::getPayload)
                .collect(Collectors.toList()))
                .containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        assertThat(messages2.stream().peek(m -> m.ack().toCompletableFuture().join())
                .map(Message::getPayload)
                .collect(Collectors.toList()))
                .containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        await().atMost(2, TimeUnit.SECONDS).until(() -> dispositionsReceived.size() >= msgCount);

        AtomicInteger count = new AtomicInteger();
        dispositionsReceived.forEach(record -> {
            int messageNum = count.get() + 1;
            assertThat(messageNum).isLessThanOrEqualTo(msgCount);

            assertThat(record.getMessageNumber()).isEqualTo(messageNum);
            assertThat(record.getState()).isInstanceOf(Accepted.class);
            assertThat(record.isSettled()).isTrue();

            count.incrementAndGet();
        });

        assertThat(count.get()).isEqualTo(msgCount);
    }

    @Test
    @Timeout(30)
    public void testABeanConsumingTheAMQPMessages() throws Exception {
        int msgCount = 10;
        List<DispositionRecord> dispositionsReceived = Collections.synchronizedList(new ArrayList<>(msgCount));

        server = setupMockServer(msgCount, dispositionsReceived, executionHolder.vertx().getDelegate());

        new MapBasedConfig()
                .put("mp.messaging.incoming.data.address", "data")
                .put("mp.messaging.incoming.data.connector", AmqpConnector.CONNECTOR_NAME)
                .put("mp.messaging.incoming.data.host", "localhost")
                .put("mp.messaging.incoming.data.port", server.actualPort())
                .put("mp.messaging.incoming.data.tracing-enabled", false)
                .write();

        ConsumptionBean bean = deploy();

        List<Integer> list = bean.getResults();

        await().atMost(5, TimeUnit.SECONDS).until(() -> list.size() >= 10);
        // ConsumptionBean adds 1, thus shifting original values by 1
        assertThat(list).containsExactly(2, 3, 4, 5, 6, 7, 8, 9, 10, 11);

        await().atMost(2, TimeUnit.SECONDS).until(() -> dispositionsReceived.size() >= msgCount);

        AtomicInteger count = new AtomicInteger();
        dispositionsReceived.forEach(record -> {
            int messageNum = count.get() + 1;
            assertThat(messageNum).isLessThanOrEqualTo(msgCount);

            assertThat(record.getMessageNumber()).isEqualTo(messageNum);
            assertThat(record.getState()).isInstanceOf(Accepted.class);
            assertThat(record.isSettled()).isTrue();

            count.incrementAndGet();
        });

        assertThat(count.get()).isEqualTo(msgCount);
    }

    @Test
    @Timeout(30)
    public void testSourceWithDataContent() throws Exception {
        doDataContentTestImpl(false);
    }

    @Test
    @Timeout(30)
    public void testSourceWithDataContentWithOctectStreamContentType() throws Exception {
        doDataContentTestImpl(true);
    }

    private void doDataContentTestImpl(boolean setContentType) throws Exception {
        List<DispositionRecord> dispositionsReceived = Collections.synchronizedList(new ArrayList<>());

        final org.apache.qpid.proton.message.Message msg = Proton.message();
        msg.setBody(new Data(new Binary("foo".getBytes(StandardCharsets.UTF_8))));
        if (setContentType) {
            msg.setContentType("application/octet-stream");
        }

        server = setupMockServerForTypeTest(msg, dispositionsReceived, executionHolder.vertx().getDelegate());

        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = getConfig(topic, server.actualPort());
        provider = new AmqpConnector();
        provider.setup(executionHolder);

        List<Message<byte[]>> messages = new ArrayList<>();
        Flow.Publisher<? extends Message<?>> publisher = provider.getPublisher(new MapBasedConfig(config));

        publisher.subscribe(createSubscriber(messages, new AtomicBoolean()));

        await().atMost(6, TimeUnit.SECONDS).until(() -> !messages.isEmpty());

        assertThat(messages.stream()
                .peek(m -> m.ack().toCompletableFuture().join())
                .map(Message::getPayload)
                .collect(Collectors.toList()))
                .containsExactly("foo".getBytes(StandardCharsets.UTF_8));

        await().atMost(2, TimeUnit.SECONDS).until(() -> !dispositionsReceived.isEmpty());

        AtomicInteger count = new AtomicInteger();
        dispositionsReceived.forEach(record -> {
            assertThat(record.getMessageNumber()).isEqualTo(1);
            assertThat(record.getState()).isInstanceOf(Accepted.class);
            assertThat(record.isSettled()).isTrue();

            count.incrementAndGet();
        });

        assertThat(count.get()).isEqualTo(1);
    }

    @Test
    @Timeout(30)
    public void testSourceWithBinaryContent() throws Exception {
        List<DispositionRecord> dispositionsReceived = Collections.synchronizedList(new ArrayList<>());

        final org.apache.qpid.proton.message.Message msg = Proton.message();
        msg.setBody(new AmqpValue(new Binary("foo".getBytes(StandardCharsets.UTF_8))));

        server = setupMockServerForTypeTest(msg, dispositionsReceived, executionHolder.vertx().getDelegate());

        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = getConfig(topic, server.actualPort());
        provider = new AmqpConnector();
        provider.setup(executionHolder);

        List<Message<byte[]>> messages = new ArrayList<>();
        Flow.Publisher<? extends Message<?>> publisher = provider.getPublisher(new MapBasedConfig(config));

        publisher.subscribe(createSubscriber(messages, new AtomicBoolean()));

        await().atMost(6, TimeUnit.SECONDS).until(() -> !messages.isEmpty());

        assertThat(messages.stream()
                .peek(m -> m.ack().toCompletableFuture().join())
                .map(Message::getPayload)
                .collect(Collectors.toList()))
                .containsExactly("foo".getBytes(StandardCharsets.UTF_8));

        await().atMost(2, TimeUnit.SECONDS).until(() -> !dispositionsReceived.isEmpty());

        AtomicInteger count = new AtomicInteger();
        dispositionsReceived.forEach(record -> {
            assertThat(record.getMessageNumber()).isEqualTo(1);
            assertThat(record.getState()).isInstanceOf(Accepted.class);
            assertThat(record.isSettled()).isTrue();

            count.incrementAndGet();
        });

        assertThat(count.get()).isEqualTo(1);
    }

    @Test
    @Timeout(30)
    public void testSourceWithJsonObjectDataContent() throws Exception {
        List<DispositionRecord> dispositionsReceived = Collections.synchronizedList(new ArrayList<>());

        JsonObject json = new JsonObject();
        String id = UUID.randomUUID().toString();
        json.put("key", id);
        json.put("some", "content");

        final org.apache.qpid.proton.message.Message msg = Proton.message();
        msg.setBody(new Data(new Binary(json.toBuffer().getBytes())));
        msg.setContentType("application/json");

        server = setupMockServerForTypeTest(msg, dispositionsReceived, executionHolder.vertx().getDelegate());

        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = getConfig(topic, server.actualPort());
        provider = new AmqpConnector();
        provider.setup(executionHolder);

        List<Message<JsonObject>> messages = new ArrayList<>();
        Flow.Publisher<? extends Message<?>> builder = provider.getPublisher(new MapBasedConfig(config));

        builder.subscribe(createSubscriber(messages, new AtomicBoolean()));

        await().atMost(6, TimeUnit.SECONDS).until(() -> !messages.isEmpty());

        Message<JsonObject> receivedMessage = messages.get(0);
        JsonObject result = receivedMessage.getPayload();
        assertThat(result).containsOnly(entry("key", id), entry("some", "content"));
        receivedMessage.ack().toCompletableFuture().join();

        await().atMost(2, TimeUnit.SECONDS).until(() -> !dispositionsReceived.isEmpty());

        AtomicInteger count = new AtomicInteger();
        dispositionsReceived.forEach(record -> {
            assertThat(record.getMessageNumber()).isEqualTo(1);
            assertThat(record.getState()).isInstanceOf(Accepted.class);
            assertThat(record.isSettled()).isTrue();

            count.incrementAndGet();
        });

        assertThat(count.get()).isEqualTo(1);
        assertThat(messages.size()).isEqualTo(1);
    }

    @Test
    @Timeout(30)
    public void testSourceWithListJsonArrayDataContent() throws Exception {
        List<DispositionRecord> dispositionsReceived = Collections.synchronizedList(new ArrayList<>());

        JsonArray list = new JsonArray();
        String id = UUID.randomUUID().toString();
        list.add("ola");
        list.add(id);

        final org.apache.qpid.proton.message.Message msg = Proton.message();
        msg.setBody(new Data(new Binary(list.toBuffer().getBytes())));
        msg.setContentType("application/json");

        server = setupMockServerForTypeTest(msg, dispositionsReceived, executionHolder.vertx().getDelegate());

        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = getConfig(topic, server.actualPort());
        provider = new AmqpConnector();
        provider.setup(executionHolder);

        List<Message<JsonArray>> messages = new ArrayList<>();
        Flow.Publisher<? extends Message<?>> builder = provider.getPublisher(new MapBasedConfig(config));

        builder.subscribe(createSubscriber(messages, new AtomicBoolean()));

        await().atMost(6, TimeUnit.SECONDS).until(() -> !messages.isEmpty());

        Message<JsonArray> receivedMessage = messages.get(0);
        JsonArray result = receivedMessage.getPayload();
        assertThat(result).containsExactly("ola", id);
        receivedMessage.ack().toCompletableFuture().join();

        await().atMost(2, TimeUnit.SECONDS).until(() -> !dispositionsReceived.isEmpty());

        AtomicInteger count = new AtomicInteger();
        dispositionsReceived.forEach(record -> {
            assertThat(record.getMessageNumber()).isEqualTo(1);
            assertThat(record.getState()).isInstanceOf(Accepted.class);
            assertThat(record.isSettled()).isTrue();

            count.incrementAndGet();
        });

        assertThat(count.get()).isEqualTo(1);
        assertThat(messages.size()).isEqualTo(1);
    }

    @Test
    @Timeout(30)
    public void testSourceWithListContent() throws Exception {
        List<DispositionRecord> dispositionsReceived = Collections.synchronizedList(new ArrayList<>());

        List<String> list = new ArrayList<>();
        String id = UUID.randomUUID().toString();
        list.add("ola");
        list.add(id);

        final org.apache.qpid.proton.message.Message msg = Proton.message();
        msg.setBody(new AmqpValue(list));

        server = setupMockServerForTypeTest(msg, dispositionsReceived, executionHolder.vertx().getDelegate());

        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = getConfig(topic, server.actualPort());
        provider = new AmqpConnector();
        provider.setup(executionHolder);

        List<Message<List<String>>> messages = new ArrayList<>();
        Flow.Publisher<? extends Message<?>> builder = provider.getPublisher(new MapBasedConfig(config));

        builder.subscribe(createSubscriber(messages, new AtomicBoolean()));

        await().atMost(6, TimeUnit.SECONDS).until(() -> !messages.isEmpty());

        Message<List<String>> receivedMessage = messages.get(0);
        List<String> result = receivedMessage.getPayload();
        assertThat(result).containsExactly("ola", id);
        receivedMessage.ack().toCompletableFuture().join();

        await().atMost(2, TimeUnit.SECONDS).until(() -> !dispositionsReceived.isEmpty());

        AtomicInteger count = new AtomicInteger();
        dispositionsReceived.forEach(record -> {
            assertThat(record.getMessageNumber()).isEqualTo(1);
            assertThat(record.getState()).isInstanceOf(Accepted.class);
            assertThat(record.isSettled()).isTrue();

            count.incrementAndGet();
        });

        assertThat(count.get()).isEqualTo(1);
        assertThat(messages.size()).isEqualTo(1);
    }

    @Test
    @Timeout(30)
    public void testSourceWithAmqpSequenceContent() throws Exception {
        List<DispositionRecord> dispositionsReceived = Collections.synchronizedList(new ArrayList<>());

        List<String> list = new ArrayList<>();
        String id = UUID.randomUUID().toString();
        list.add("ola");
        list.add(id);

        final org.apache.qpid.proton.message.Message msg = Proton.message();
        msg.setBody(new AmqpSequence(list));

        server = setupMockServerForTypeTest(msg, dispositionsReceived, executionHolder.vertx().getDelegate());

        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = getConfig(topic, server.actualPort());
        provider = new AmqpConnector();
        provider.setup(executionHolder);

        List<Message<List<String>>> messages = new ArrayList<>();
        Flow.Publisher<? extends Message<?>> builder = provider.getPublisher(new MapBasedConfig(config));

        builder.subscribe(createSubscriber(messages, new AtomicBoolean()));

        await().atMost(6, TimeUnit.SECONDS).until(() -> !messages.isEmpty());

        Message<List<String>> receivedMessage = messages.get(0);
        List<String> result = receivedMessage.getPayload();
        assertThat(result).containsExactly("ola", id);
        receivedMessage.ack().toCompletableFuture().join();

        await().atMost(2, TimeUnit.SECONDS).until(() -> !dispositionsReceived.isEmpty());

        AtomicInteger count = new AtomicInteger();
        dispositionsReceived.forEach(record -> {
            assertThat(record.getMessageNumber()).isEqualTo(1);
            assertThat(record.getState()).isInstanceOf(Accepted.class);
            assertThat(record.isSettled()).isTrue();

            count.incrementAndGet();
        });

        assertThat(count.get()).isEqualTo(1);
        assertThat(messages.size()).isEqualTo(1);
    }

    private MockServer setupMockServer(int msgCount, List<DispositionRecord> dispositions, Vertx vertx) throws Exception {
        return setupMockServer(msgCount, dispositions, vertx, new CountDownLatch(0));
    }

    private MockServer setupMockServer(int msgCount, List<DispositionRecord> dispositions, Vertx vertx,
            final CountDownLatch startSendLatch) throws Exception {
        AtomicInteger sent = new AtomicInteger(1);

        return new MockServer(vertx, serverConnection -> {
            serverConnection.openHandler(serverSender -> {
                serverConnection.closeHandler(x -> serverConnection.close());
                serverConnection.open();
            });

            serverConnection.sessionOpenHandler(serverSession -> {
                serverSession.closeHandler(x -> serverSession.close());
                serverSession.open();
            });

            serverConnection.senderOpenHandler(serverSender -> {
                serverSender.sendQueueDrainHandler(x -> {

                    // Naughty, but should be fine for the test, normally taking 0.0X seconds total.
                    try {
                        if (!startSendLatch.await(6, TimeUnit.SECONDS)) {
                            LOGGER.error("Test server did not get signal to start sending in alotted time");
                            return;
                        }
                    } catch (InterruptedException ie) {
                        LOGGER.warn("Test server interrupted awaiting signal to start sending");
                        return;
                    }

                    while (sent.get() <= msgCount && !serverSender.sendQueueFull()) {
                        final org.apache.qpid.proton.message.Message m = Proton.message();
                        final int i = sent.getAndIncrement();
                        m.setBody(new AmqpValue(i));

                        serverSender.send(m, delivery -> {
                            DeliveryState deliveryState = delivery.getRemoteState();
                            dispositions.add(new DispositionRecord(i, deliveryState, delivery.remotelySettled()));
                        });
                    }
                });

                serverSender.open();
            });
        });
    }

    private MockServer setupMockServerForTypeTest(org.apache.qpid.proton.message.Message msg,
            List<DispositionRecord> dispositions, Vertx vertx) throws Exception {
        return new MockServer(vertx, serverConnection -> {
            serverConnection.openHandler(serverSender -> {
                serverConnection.closeHandler(x -> serverConnection.close());
                serverConnection.open();
            });

            serverConnection.sessionOpenHandler(serverSession -> {
                serverSession.closeHandler(x -> serverSession.close());
                serverSession.open();
            });

            serverConnection.senderOpenHandler(serverSender -> {
                serverSender.open();

                // Just immediately buffer a single send.
                serverSender.send(msg, delivery -> {
                    DeliveryState deliveryState = delivery.getRemoteState();
                    dispositions.add(new DispositionRecord(1, deliveryState, delivery.remotelySettled()));
                });
            });
        });
    }

    private ConsumptionBean deploy() {
        Weld weld = new Weld();
        weld.addBeanClass(ConsumptionBean.class);

        container = weld.initialize();
        return container.getBeanManager().createInstance().select(ConsumptionBean.class).get();
    }

    @NotNull
    private Map<String, Object> getConfig(String topic, int port) {
        Map<String, Object> config = new HashMap<>();
        config.put("address", topic);
        config.put(CHANNEL_NAME_ATTRIBUTE, UUID.randomUUID().toString());
        config.put("host", "localhost");
        config.put("port", port);
        config.put("name", "some name");
        config.put("tracing-enabled", false);
        return config;
    }

    @NotNull
    private Map<String, Object> getConfigUsingChannelName(String topic, int port) {
        Map<String, Object> config = new HashMap<>();
        config.put(CHANNEL_NAME_ATTRIBUTE, topic);
        config.put("host", "localhost");
        config.put("port", port);
        config.put("name", "some name");
        config.put("tracing-enabled", false);
        return config;
    }

}
