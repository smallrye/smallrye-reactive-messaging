package io.smallrye.reactive.messaging.amqp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.awaitility.Awaitility.await;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.amqp.transport.Target;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorFactory;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.reactivestreams.Subscriber;

import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.buffer.Buffer;

public class AmqpSinkTest extends AmqpTestBase {

    private static final String FOO = "foo";
    private static final String ID = "id";
    private static final String HELLO = "hello-";
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

        MapBasedConfig.cleanup();
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
    }

    @Test
    @Timeout(30)
    public void testSinkUsingIntegerUsingDefaultAnonymousSender() throws Exception {
        int msgCount = 10;
        String topic = UUID.randomUUID().toString();
        CountDownLatch msgsReceived = new CountDownLatch(msgCount);
        List<org.apache.qpid.proton.message.Message> messagesReceived = Collections
                .synchronizedList(new ArrayList<>(msgCount));
        AtomicReference<String> attachAddress = new AtomicReference<>("non-null-initialisation-value");

        server = setupMockServerForTypeTest(messagesReceived, msgsReceived, attachAddress);

        Flow.Subscriber<? extends Message<?>> sink = createProviderAndSink(topic, server.actualPort());
        //noinspection unchecked
        Multi.createFrom().range(0, msgCount)
                .map(Message::of)
                .subscribe((Flow.Subscriber<? super Message<Integer>>) sink);

        assertThat(msgsReceived.await(6, TimeUnit.SECONDS)).isTrue();

        List<Object> payloadsReceived = new ArrayList<>(msgCount);
        messagesReceived.forEach(msg -> {
            Section body = msg.getBody();
            assertThat(body).isInstanceOf(AmqpValue.class);
            payloadsReceived.add(((AmqpValue) body).getValue());
        });

        assertThat(payloadsReceived).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        // Should have used an anonymous sender link, verify null target address
        assertThat(attachAddress.get()).isNull();
    }

    @Test
    @Timeout(30)
    public void testSinkUsingIntegerUsingNonAnonymousSender() throws Exception {
        int msgCount = 10;
        String topic = UUID.randomUUID().toString();
        CountDownLatch msgsReceived = new CountDownLatch(msgCount);
        List<org.apache.qpid.proton.message.Message> messagesReceived = Collections
                .synchronizedList(new ArrayList<>(msgCount));
        AtomicReference<String> attachAddress = new AtomicReference<>("non-null-initialisation-value");

        server = setupMockServerForTypeTest(messagesReceived, msgsReceived, attachAddress);

        Flow.Subscriber<? extends Message<?>> sink = createProviderAndNonAnonymousSink(topic,
                server.actualPort());
        //noinspection unchecked
        Multi.createFrom().range(0, 10)
                .map(Message::of)
                .subscribe((Flow.Subscriber<? super Message<Integer>>) sink);

        assertThat(msgsReceived.await(6, TimeUnit.SECONDS)).isTrue();

        List<Object> payloadsReceived = new ArrayList<>(msgCount);
        messagesReceived.forEach(msg -> {
            Section body = msg.getBody();
            assertThat(body).isInstanceOf(AmqpValue.class);
            payloadsReceived.add(((AmqpValue) body).getValue());
        });

        assertThat(payloadsReceived).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        // Should have used a fixed-address sender link, vertify target address
        assertThat(attachAddress.get()).isEqualTo(topic);
    }

    @Test
    @Timeout(30)
    public void testSinkUsingString() throws Exception {
        int msgCount = 10;
        CountDownLatch msgsReceived = new CountDownLatch(msgCount);
        List<org.apache.qpid.proton.message.Message> messagesReceived = Collections
                .synchronizedList(new ArrayList<>(msgCount));

        server = setupMockServerForTypeTest(messagesReceived, msgsReceived);

        Flow.Subscriber<? extends Message<?>> sink = createProviderAndNonAnonymousSink(
                UUID.randomUUID().toString(),
                server.actualPort());
        //noinspection unchecked
        Multi.createFrom().range(0, 10)
                .map(i -> Integer.toString(i))
                .map(Message::of)
                .subscribe((Flow.Subscriber<? super Message<String>>) sink);

        assertThat(msgsReceived.await(6, TimeUnit.SECONDS)).isTrue();

        List<Object> payloadsReceived = new ArrayList<>(msgCount);
        messagesReceived.forEach(msg -> {
            Section body = msg.getBody();
            assertThat(body).isInstanceOf(AmqpValue.class);
            payloadsReceived.add(((AmqpValue) body).getValue());
        });

        assertThat(payloadsReceived).containsExactly("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
    }

    private MockServer setupMockServerForTypeTest(List<org.apache.qpid.proton.message.Message> messages,
            CountDownLatch latch)
            throws Exception {
        //noinspection Convert2Diamond
        return setupMockServerForTypeTest(messages, latch, new AtomicReference<String>());
    }

    private MockServer setupMockServerForTypeTest(List<org.apache.qpid.proton.message.Message> messages,
            CountDownLatch latch,
            AtomicReference<String> attachAddress) throws Exception {
        return new MockServer(executionHolder.vertx().getDelegate(), serverConnection -> {
            serverConnection.openHandler(serverSender -> {
                serverConnection.closeHandler(x -> serverConnection.close());
                serverConnection.open();
            });

            serverConnection.sessionOpenHandler(serverSession -> {
                serverSession.closeHandler(x -> serverSession.close());
                serverSession.open();
            });

            serverConnection.receiverOpenHandler(serverReceiver -> {
                Target remoteTarget = serverReceiver.getRemoteTarget();
                attachAddress.set(remoteTarget.getAddress());
                serverReceiver.setTarget(remoteTarget.copy());

                serverReceiver.handler((delivery, message) -> {
                    delivery.disposition(Accepted.getInstance(), true);
                    messages.add(message);

                    latch.countDown();
                });

                serverReceiver.open();
            });
        });
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
    @Timeout(30)
    public void testSinkUsingObject() throws Exception {
        int msgCount = 10;
        CountDownLatch msgsReceived = new CountDownLatch(msgCount);
        List<org.apache.qpid.proton.message.Message> messagesReceived = Collections
                .synchronizedList(new ArrayList<>(msgCount));

        server = setupMockServerForTypeTest(messagesReceived, msgsReceived);

        Flow.Subscriber<? extends Message<?>> sink = createProviderAndSink(UUID.randomUUID().toString(),
                server.actualPort());
        //noinspection unchecked
        Multi.createFrom().range(0, 10)
                .map(i -> {
                    Person p = new Person();
                    p.setName(HELLO + i);
                    return p;
                })
                .map(Message::of)
                .subscribe((Flow.Subscriber<? super Message<Person>>) sink);

        assertThat(msgsReceived.await(6, TimeUnit.SECONDS)).isTrue();

        AtomicInteger count = new AtomicInteger();
        messagesReceived.forEach(msg -> {
            assertThat(msg.getContentType()).isEqualTo("application/json");

            Section body = msg.getBody();
            assertThat(body).isInstanceOf(Data.class);
            Binary bin = ((Data) body).getValue();
            byte[] bytes = Binary.copy(bin).getArray();
            Person p = Buffer.buffer(bytes).toJsonObject().mapTo(Person.class);

            assertThat(p.getName()).isEqualTo(HELLO + count.get());

            count.incrementAndGet();
        });

        assertThat(count.get()).isEqualTo(msgCount);
    }

    /**
     * Extension of Person that cannot be serialized to JSON.
     */
    @SuppressWarnings("unused")
    public static class Bad extends Person {

        public Person getPerson() {
            return this;
        }

    }

    @Test
    @Timeout(30)
    public void testSinkUsingObjectThatCannotBeSerialized() throws Exception {
        int msgCount = 10;
        AtomicInteger nack = new AtomicInteger();
        CountDownLatch msgsReceived = new CountDownLatch(msgCount / 2);
        List<org.apache.qpid.proton.message.Message> messagesReceived = Collections
                .synchronizedList(new ArrayList<>(msgCount));

        server = setupMockServerForTypeTest(messagesReceived, msgsReceived);

        Flow.Subscriber<? extends Message<?>> sink = createProviderAndSink(UUID.randomUUID().toString(),
                server.actualPort());
        //noinspection unchecked
        Multi.createFrom().range(0, 10)
                .map(i -> {
                    Person p;
                    if (i % 2 == 0) {
                        p = new Person();
                        p.setName(HELLO + i);
                    } else {
                        p = new Bad();
                        p.setName(HELLO + i);
                    }
                    return p;
                })
                .map(p -> Message.of(p, () -> CompletableFuture.completedFuture(null), e -> {
                    nack.incrementAndGet();
                    return CompletableFuture.completedFuture(null);
                }))
                .subscribe((Flow.Subscriber<? super Message<Person>>) sink);

        assertThat(msgsReceived.await(6, TimeUnit.SECONDS)).isTrue();
        await().until(() -> nack.get() == 5);

        AtomicInteger count = new AtomicInteger();
        messagesReceived.forEach(msg -> {
            assertThat(msg.getContentType()).isEqualTo("application/json");

            Section body = msg.getBody();
            assertThat(body).isInstanceOf(Data.class);
            Binary bin = ((Data) body).getValue();
            byte[] bytes = Binary.copy(bin).getArray();
            JsonObject json = Buffer.buffer(bytes).toJsonObject();
            Person p = json.mapTo(Person.class);
            assertThat(p.getName()).startsWith("hello-");
            count.incrementAndGet();
        });

        assertThat(count.get()).isEqualTo(msgCount / 2);
        assertThat(nack).hasValue(5);
    }

    @Test
    @Timeout(30)
    public void testSinkUsingJsonObject() throws Exception {
        int msgCount = 10;
        CountDownLatch msgsReceived = new CountDownLatch(msgCount);
        List<org.apache.qpid.proton.message.Message> messagesReceived = Collections
                .synchronizedList(new ArrayList<>(msgCount));

        server = setupMockServerForTypeTest(messagesReceived, msgsReceived);

        Flow.Subscriber<? extends Message<?>> sink = createProviderAndSink(UUID.randomUUID().toString(),
                server.actualPort());
        //noinspection unchecked
        Multi.createFrom().range(0, 10)
                .map(i -> new JsonObject().put(ID, HELLO + i))
                .map(Message::of)
                .subscribe((Flow.Subscriber<? super Message<?>>) sink);

        assertThat(msgsReceived.await(6, TimeUnit.SECONDS)).isTrue();

        AtomicInteger count = new AtomicInteger();
        messagesReceived.forEach(msg -> {
            assertThat(msg.getContentType()).isEqualTo("application/json");

            Section body = msg.getBody();
            assertThat(body).isInstanceOf(Data.class);
            Binary bin = ((Data) body).getValue();
            byte[] bytes = Binary.copy(bin).getArray();
            JsonObject json = Buffer.buffer(bytes).toJsonObject();

            assertThat(json.getString(ID)).isEqualTo(HELLO + count.get());

            count.incrementAndGet();
        });

        assertThat(count.get()).isEqualTo(msgCount);
    }

    @Test
    @Timeout(30)
    public void testSinkUsingJsonArray() throws Exception {
        int msgCount = 10;
        CountDownLatch msgsReceived = new CountDownLatch(msgCount);
        List<org.apache.qpid.proton.message.Message> messagesReceived = Collections
                .synchronizedList(new ArrayList<>(msgCount));

        server = setupMockServerForTypeTest(messagesReceived, msgsReceived);

        Flow.Subscriber<? extends Message<?>> sink = createProviderAndSink(UUID.randomUUID().toString(),
                server.actualPort());
        //noinspection unchecked
        Multi.createFrom().range(0, 10)
                .map(i -> new JsonArray().add(HELLO + i).add(FOO))
                .map(Message::of)
                .subscribe((Flow.Subscriber<? super Message<?>>) sink);

        assertThat(msgsReceived.await(6, TimeUnit.SECONDS)).isTrue();

        AtomicInteger count = new AtomicInteger();
        messagesReceived.forEach(msg -> {
            assertThat(msg.getContentType()).isEqualTo("application/json");

            Section body = msg.getBody();
            assertThat(body).isInstanceOf(Data.class);
            Binary bin = ((Data) body).getValue();
            byte[] bytes = Binary.copy(bin).getArray();
            JsonArray json = Buffer.buffer(bytes).toJsonArray();

            assertThat(json.getString(0)).isEqualTo(HELLO + count.get());
            assertThat(json.getString(1)).isEqualTo(FOO);

            count.incrementAndGet();
        });

        assertThat(count.get()).isEqualTo(msgCount);
    }

    @Test
    @Timeout(30)
    public void testSinkUsingList() throws Exception {
        int msgCount = 10;
        CountDownLatch msgsReceived = new CountDownLatch(msgCount);
        List<org.apache.qpid.proton.message.Message> messagesReceived = Collections
                .synchronizedList(new ArrayList<>(msgCount));

        server = setupMockServerForTypeTest(messagesReceived, msgsReceived);

        Flow.Subscriber<? extends Message<?>> sink = createProviderAndSink(UUID.randomUUID().toString(),
                server.actualPort());
        //noinspection unchecked
        Multi.createFrom().range(0, 10)
                .map(i -> {
                    List<String> res = new ArrayList<>();
                    res.add(HELLO + i);
                    res.add(FOO);
                    return res;
                })
                .map(Message::of)
                .subscribe((Flow.Subscriber<? super Message<?>>) sink);

        assertThat(msgsReceived.await(6, TimeUnit.SECONDS)).isTrue();

        AtomicInteger count = new AtomicInteger();
        messagesReceived.forEach(msg -> {
            assertThat(msg.getContentType()).isNull();

            Section body = msg.getBody();
            assertThat(body).isInstanceOf(AmqpValue.class);
            assertThat(((AmqpValue) body).getValue()).isInstanceOf(List.class);
            List<?> list = (List<?>) ((AmqpValue) body).getValue();

            assertThat(list.get(0)).isEqualTo(HELLO + count.get());
            assertThat(list.get(1)).isEqualTo(FOO);

            count.incrementAndGet();
        });

        assertThat(count.get()).isEqualTo(msgCount);
    }

    @Test
    @Timeout(30)
    public void testSinkUsingVertxBuffer() throws Exception {
        int msgCount = 10;
        CountDownLatch msgsReceived = new CountDownLatch(msgCount);
        List<org.apache.qpid.proton.message.Message> messagesReceived = Collections
                .synchronizedList(new ArrayList<>(msgCount));

        server = setupMockServerForTypeTest(messagesReceived, msgsReceived);

        Flow.Subscriber<? extends Message<?>> sink = createProviderAndSink(UUID.randomUUID().toString(),
                server.actualPort());
        //noinspection unchecked
        Multi.createFrom().range(0, 10)
                .map(i -> new JsonObject().put(ID, HELLO + i).toBuffer())
                .map(Message::of)
                .subscribe((Flow.Subscriber<? super Message<?>>) sink);

        assertThat(msgsReceived.await(6, TimeUnit.SECONDS)).isTrue();

        AtomicInteger count = new AtomicInteger();
        messagesReceived.forEach(msg -> {
            assertThat(msg.getContentType()).isEqualTo("application/octet-stream");

            Section body = msg.getBody();
            assertThat(body).isInstanceOf(Data.class);
            byte[] receievedBytes = Binary.copy(((Data) body).getValue()).getArray();

            byte[] expectedBytes = new JsonObject().put(ID, HELLO + count.get()).toBuffer().getBytes();
            assertThat(receievedBytes).isEqualTo(expectedBytes);

            count.incrementAndGet();
        });

        assertThat(count.get()).isEqualTo(msgCount);
    }

    @Test
    @Timeout(30)
    public void testSinkUsingMutinyBuffer() throws Exception {
        int msgCount = 10;
        CountDownLatch msgsReceived = new CountDownLatch(msgCount);
        List<org.apache.qpid.proton.message.Message> messagesReceived = Collections
                .synchronizedList(new ArrayList<>(msgCount));

        server = setupMockServerForTypeTest(messagesReceived, msgsReceived);

        Flow.Subscriber<? extends Message<?>> sink = createProviderAndSink(UUID.randomUUID().toString(),
                server.actualPort());
        //noinspection unchecked
        Multi.createFrom().range(0, 10)
                .map(i -> new Buffer(new JsonObject().put(ID, HELLO + i).toBuffer()))
                .map(Message::of)
                .subscribe((Flow.Subscriber<? super Message<?>>) sink);

        assertThat(msgsReceived.await(6, TimeUnit.SECONDS)).isTrue();

        AtomicInteger count = new AtomicInteger();
        messagesReceived.forEach(msg -> {
            assertThat(msg.getContentType()).isEqualTo("application/octet-stream");

            Section body = msg.getBody();
            assertThat(body).isInstanceOf(Data.class);
            byte[] receievedBytes = Binary.copy(((Data) body).getValue()).getArray();

            byte[] expectedBytes = new JsonObject().put(ID, HELLO + count.get()).toBuffer().getBytes();
            assertThat(receievedBytes).isEqualTo(expectedBytes);

            count.incrementAndGet();
        });

        assertThat(count.get()).isEqualTo(msgCount);
    }

    @Test
    @Timeout(30)
    public void testSinkUsingByteArray() throws Exception {
        int msgCount = 10;
        CountDownLatch msgsReceived = new CountDownLatch(msgCount);
        List<org.apache.qpid.proton.message.Message> messagesReceived = Collections
                .synchronizedList(new ArrayList<>(msgCount));

        server = setupMockServerForTypeTest(messagesReceived, msgsReceived);

        Flow.Subscriber<? extends Message<?>> sink = createProviderAndSink(UUID.randomUUID().toString(),
                server.actualPort());
        //noinspection unchecked
        Multi.createFrom().range(0, 10)
                .map(i -> new JsonObject().put(ID, HELLO + i).toBuffer().getBytes())
                .map(Message::of)
                .subscribe((Flow.Subscriber<? super Message<?>>) sink);

        assertThat(msgsReceived.await(6, TimeUnit.SECONDS)).isTrue();

        AtomicInteger count = new AtomicInteger();
        messagesReceived.forEach(msg -> {
            assertThat(msg.getContentType()).isEqualTo("application/octet-stream");

            Section body = msg.getBody();
            assertThat(body).isInstanceOf(Data.class);
            byte[] receievedBytes = Binary.copy(((Data) body).getValue()).getArray();

            byte[] expectedBytes = new JsonObject().put(ID, HELLO + count.get()).toBuffer().getBytes();
            assertThat(receievedBytes).isEqualTo(expectedBytes);

            count.incrementAndGet();
        });

        assertThat(count.get()).isEqualTo(msgCount);
    }

    @Test
    @Timeout(30)
    public void testABeanProducingMessagesSentToAMQP() throws Exception {
        int msgCount = 10;
        String address = "sink";
        CountDownLatch msgsReceived = new CountDownLatch(msgCount);
        List<org.apache.qpid.proton.message.Message> messagesReceived = Collections
                .synchronizedList(new ArrayList<>(msgCount));
        AtomicReference<String> attachAddress = new AtomicReference<>("non-null-initialisation-value");

        server = setupMockServerForTypeTest(messagesReceived, msgsReceived, attachAddress);

        Weld weld = new Weld();
        weld.addBeanClass(ProducingBean.class);

        new MapBasedConfig()
                .with("mp.messaging.outgoing.sink.address", address)
                .with("mp.messaging.outgoing.sink.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.sink.host", "localhost")
                .with("mp.messaging.outgoing.sink.port", server.actualPort())
                .with("mp.messaging.outgoing.sink.tracing-enabled", false)
                .write();

        container = weld.initialize();

        assertThat(msgsReceived.await(6, TimeUnit.SECONDS)).isTrue();

        List<Object> payloadsReceived = new ArrayList<>(msgCount);
        messagesReceived.forEach(msg -> {
            assertThat(msg.getAddress()).isEqualTo(address);
            assertThat(msg.getSubject()).isNull();

            Section body = msg.getBody();
            assertThat(body).isInstanceOf(AmqpValue.class);
            payloadsReceived.add(((AmqpValue) body).getValue());
        });

        assertThat(payloadsReceived).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        // Should have used an anonymous sender link, verify null target address
        assertThat(attachAddress.get()).isNull();
    }

    @Test
    @Timeout(30)
    public void testABeanProducingMessagesSentToAMQPWithOutboundMetadata() throws Exception {
        int msgCount = 10;
        CountDownLatch msgsReceived = new CountDownLatch(msgCount);
        List<org.apache.qpid.proton.message.Message> messagesReceived = Collections
                .synchronizedList(new ArrayList<>(msgCount));
        AtomicReference<String> attachAddress = new AtomicReference<>("non-null-initialisation-value");

        server = setupMockServerForTypeTest(messagesReceived, msgsReceived, attachAddress);

        Weld weld = new Weld();
        weld.addBeanClass(ProducingBeanUsingOutboundMetadata.class);

        new MapBasedConfig()
                .with("mp.messaging.outgoing.sink.address", "not-used")
                .with("mp.messaging.outgoing.sink.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.sink.host", "localhost")
                .with("mp.messaging.outgoing.sink.port", server.actualPort())
                .with("mp.messaging.outgoing.sink.tracing-enabled", false)
                .write();

        container = weld.initialize();

        assertThat(msgsReceived.await(6, TimeUnit.SECONDS)).isTrue();

        List<Object> payloadsReceived = new ArrayList<>(msgCount);
        messagesReceived.forEach(msg -> {
            assertThat(msg.getAddress()).isEqualTo("metadata-address"); // Set in ProducingBeanUsingOutboundMetadata
            assertThat(msg.getSubject()).isEqualTo("metadata-subject");

            Section body = msg.getBody();
            assertThat(body).isInstanceOf(AmqpValue.class);
            payloadsReceived.add(((AmqpValue) body).getValue());
        });

        assertThat(payloadsReceived).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        // Should have used an anonymous sender link, verify null target address
        assertThat(attachAddress.get()).isNull();
    }

    @Test
    @Timeout(30)
    public void testABeanProducingMessagesSentToAMQPWithOutboundMetadataUsingNonAnonymousSender() throws Exception {
        int msgCount = 10;
        String address = "sink-foo";
        CountDownLatch msgsReceived = new CountDownLatch(msgCount);
        List<org.apache.qpid.proton.message.Message> messagesReceived = Collections
                .synchronizedList(new ArrayList<>(msgCount));
        AtomicReference<String> attachAddress = new AtomicReference<>("non-null-initialisation-value");

        server = setupMockServerForTypeTest(messagesReceived, msgsReceived, attachAddress);

        Weld weld = new Weld();
        weld.addBeanClass(ProducingBeanUsingOutboundMetadata.class);

        new MapBasedConfig()
                .with("mp.messaging.outgoing.sink.address", address)
                .with("mp.messaging.outgoing.sink.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.sink.host", "localhost")
                .with("mp.messaging.outgoing.sink.port", server.actualPort())
                .with("mp.messaging.outgoing.sink.use-anonymous-sender", false)
                .with("mp.messaging.outgoing.sink.tracing-enabled", false)
                .write();

        container = weld.initialize();

        assertThat(msgsReceived.await(6, TimeUnit.SECONDS)).isTrue();

        List<Object> payloadsReceived = new ArrayList<>(msgCount);
        messagesReceived.forEach(msg -> {
            assertThat(msg.getAddress()).isEqualTo(
                    address); // Matches the one from the config, not metadata, due to not using an anonymous sender
            assertThat(msg.getSubject()).isEqualTo("metadata-subject");

            Section body = msg.getBody();
            assertThat(body).isInstanceOf(AmqpValue.class);
            payloadsReceived.add(((AmqpValue) body).getValue());
        });

        assertThat(payloadsReceived).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        // Should have used an fixed-address sender link, verify target address
        assertThat(attachAddress.get()).isEqualTo(address);
    }

    @Test
    @Timeout(30)
    @SuppressWarnings("deprecation")
    public void testSinkUsingAmqpMessage() throws Exception {
        int msgCount = 10;
        CountDownLatch msgsReceived = new CountDownLatch(msgCount);
        List<org.apache.qpid.proton.message.Message> messagesReceived = Collections
                .synchronizedList(new ArrayList<>(msgCount));

        server = setupMockServerForTypeTest(messagesReceived, msgsReceived);

        Flow.Subscriber<? extends Message<?>> sink = createProviderAndSink(UUID.randomUUID().toString(),
                server.actualPort());

        //noinspection unchecked
        Multi.createFrom().range(0, 10)
                .map(v -> AmqpMessage.<String> builder()
                        .withBody(HELLO + v)
                        .withSubject("foo")
                        .build())
                .subscribe((Flow.Subscriber<? super Message<?>>) sink);

        assertThat(msgsReceived.await(6, TimeUnit.SECONDS)).isTrue();

        AtomicInteger count = new AtomicInteger();
        messagesReceived.forEach(msg -> {
            assertThat(msg.getContentType()).isNull();
            assertThat(msg.getSubject()).isEqualTo("foo");

            Section body = msg.getBody();
            assertThat(body).isInstanceOf(AmqpValue.class);
            Object payload = ((AmqpValue) body).getValue();

            assertThat(HELLO + count).isEqualTo(payload);

            count.incrementAndGet();
        });

        assertThat(count.get()).isEqualTo(msgCount);
    }

    @Test
    @Timeout(30)
    @SuppressWarnings("deprecation")
    public void testSinkUsingAmqpMessageWithNonAnonymousSender() throws Exception {
        int msgCount = 10;
        CountDownLatch msgsReceived = new CountDownLatch(msgCount);
        List<org.apache.qpid.proton.message.Message> messagesReceived = Collections
                .synchronizedList(new ArrayList<>(msgCount));
        AtomicReference<String> attachAddress = new AtomicReference<>("non-null-initialisation-value");

        server = setupMockServerForTypeTest(messagesReceived, msgsReceived, attachAddress);

        String address = UUID.randomUUID().toString();
        Flow.Subscriber<? extends Message<?>> sink = createProviderAndNonAnonymousSink(address,
                server.actualPort());

        //noinspection unchecked
        Multi.createFrom().range(0, 10)
                .map(v -> AmqpMessage.<String> builder()
                        .withBody(HELLO + v)
                        .withSubject("foo")
                        .withAddress("unused")
                        .build())
                .subscribe((Flow.Subscriber<? super AmqpMessage<String>>) sink);

        assertThat(msgsReceived.await(6, TimeUnit.SECONDS)).isTrue();

        AtomicInteger count = new AtomicInteger();
        messagesReceived.forEach(msg -> {
            assertThat(msg.getContentType()).isNull();
            assertThat(msg.getSubject()).isEqualTo("foo");
            assertThat(msg.getAddress()).isEqualTo(
                    address); // Matches the one from the config, not message, due to not using an anonymous sender

            Section body = msg.getBody();
            assertThat(body).isInstanceOf(AmqpValue.class);
            Object payload = ((AmqpValue) body).getValue();

            assertThat(HELLO + count).isEqualTo(payload);

            count.incrementAndGet();
        });

        assertThat(count.get()).isEqualTo(msgCount);

        // Should have used an fixed-address sender link, verify target address
        assertThat(attachAddress.get()).isEqualTo(address);
    }

    @Test
    @Timeout(30)
    public void testSinkUsingProtonMessage() throws Exception {
        int msgCount = 10;
        CountDownLatch msgsReceived = new CountDownLatch(msgCount);
        List<org.apache.qpid.proton.message.Message> messagesReceived = Collections
                .synchronizedList(new ArrayList<>(msgCount));

        server = setupMockServerForTypeTest(messagesReceived, msgsReceived);

        Flow.Subscriber<? extends Message<?>> sink = createProviderAndSink(UUID.randomUUID().toString(),
                server.actualPort());

        //noinspection unchecked
        Multi.createFrom().range(0, 10)
                .map(v -> {
                    org.apache.qpid.proton.message.Message message = org.apache.qpid.proton.message.Message.Factory
                            .create();
                    message.setBody(new Data(new Binary((HELLO + v).getBytes(StandardCharsets.UTF_8))));
                    message.setContentType("text/plain");
                    message.setSubject("bar");
                    return Message.of(message);
                })
                .subscribe((Flow.Subscriber<? super Message<?>>) sink);

        assertThat(msgsReceived.await(6, TimeUnit.SECONDS)).isTrue();

        AtomicInteger count = new AtomicInteger();
        messagesReceived.forEach(msg -> {
            assertThat(msg.getContentType()).isEqualTo("text/plain");
            assertThat(msg.getSubject()).isEqualTo("bar");

            Section body = msg.getBody();
            assertThat(body).isInstanceOf(Data.class);
            byte[] receievedBytes = Binary.copy(((Data) body).getValue()).getArray();

            byte[] expectedBytes = (HELLO + count.get()).getBytes(StandardCharsets.UTF_8);
            assertThat(receievedBytes).isEqualTo(expectedBytes);

            count.incrementAndGet();
        });

        assertThat(count.get()).isEqualTo(msgCount);
    }

    @Test
    @Timeout(30)
    public void testSinkUsingMutinyMessage() throws Exception {
        int msgCount = 10;
        CountDownLatch msgsReceived = new CountDownLatch(msgCount);
        List<org.apache.qpid.proton.message.Message> messagesReceived = Collections
                .synchronizedList(new ArrayList<>(msgCount));

        server = setupMockServerForTypeTest(messagesReceived, msgsReceived);

        Flow.Subscriber<? extends Message<?>> sink = createProviderAndSink(UUID.randomUUID().toString(),
                server.actualPort());

        Multi.createFrom().range(0, 10)
                .map(v -> io.vertx.mutiny.amqp.AmqpMessage.create()
                        .withBufferAsBody(Buffer.buffer((HELLO + v).getBytes(StandardCharsets.UTF_8)))
                        .contentType("text/plain")
                        .subject("bar")
                        .build())
                .map(Message::of)
                .subscribe((Flow.Subscriber<? super Message<io.vertx.mutiny.amqp.AmqpMessage>>) sink);

        assertThat(msgsReceived.await(6, TimeUnit.SECONDS)).isTrue();

        AtomicInteger count = new AtomicInteger();
        messagesReceived.forEach(msg -> {
            assertThat(msg.getContentType()).isEqualTo("text/plain");
            assertThat(msg.getSubject()).isEqualTo("bar");

            Section body = msg.getBody();
            assertThat(body).isInstanceOf(Data.class);
            byte[] receievedBytes = Binary.copy(((Data) body).getValue()).getArray();

            byte[] expectedBytes = (HELLO + count.get()).getBytes(StandardCharsets.UTF_8);
            assertThat(receievedBytes).isEqualTo(expectedBytes);

            count.incrementAndGet();
        });

        assertThat(count.get()).isEqualTo(msgCount);
    }

    @Test
    @Timeout(30)
    public void testSinkUsingMutinyMessageViaBuilder() throws Exception {
        int msgCount = 10;
        CountDownLatch msgsReceived = new CountDownLatch(msgCount);
        List<org.apache.qpid.proton.message.Message> messagesReceived = Collections
                .synchronizedList(new ArrayList<>(msgCount));

        server = setupMockServerForTypeTest(messagesReceived, msgsReceived);

        Flow.Subscriber<? extends Message<?>> sink = createProviderAndSink(UUID.randomUUID().toString(),
                server.actualPort());

        Multi.createFrom().range(0, 10)
                .map(v -> {
                    io.vertx.mutiny.amqp.AmqpMessageBuilder builder = io.vertx.mutiny.amqp.AmqpMessageBuilder.create();
                    builder.withBufferAsBody(Buffer.buffer((HELLO + v).getBytes(StandardCharsets.UTF_8)))
                            .contentType("text/plain")
                            .subject("baz");
                    return Message.of(builder.build());
                })
                .subscribe((Flow.Subscriber<? super Message<?>>) sink);

        assertThat(msgsReceived.await(6, TimeUnit.SECONDS)).isTrue();

        AtomicInteger count = new AtomicInteger();
        messagesReceived.forEach(msg -> {
            assertThat(msg.getContentType()).isEqualTo("text/plain");
            assertThat(msg.getSubject()).isEqualTo("baz");

            Section body = msg.getBody();
            assertThat(body).isInstanceOf(Data.class);
            byte[] receievedBytes = Binary.copy(((Data) body).getValue()).getArray();

            byte[] expectedBytes = (HELLO + count.get()).getBytes(StandardCharsets.UTF_8);
            assertThat(receievedBytes).isEqualTo(expectedBytes);

            count.incrementAndGet();
        });

        assertThat(count.get()).isEqualTo(msgCount);
    }

    @Test
    @Timeout(30)
    public void testSinkUsingVertxAmqpClientMessage() throws Exception {
        int msgCount = 10;
        CountDownLatch msgsReceived = new CountDownLatch(msgCount);
        List<org.apache.qpid.proton.message.Message> messagesReceived = Collections
                .synchronizedList(new ArrayList<>(msgCount));

        server = setupMockServerForTypeTest(messagesReceived, msgsReceived);

        Flow.Subscriber<? extends Message<?>> sink = createProviderAndSink(UUID.randomUUID().toString(),
                server.actualPort());

        Multi.createFrom().range(0, 10)
                .map(v -> {
                    io.vertx.amqp.AmqpMessageBuilder builder = io.vertx.amqp.AmqpMessageBuilder.create();
                    builder.subject("baz")
                            .withBufferAsBody(io.vertx.core.buffer.Buffer.buffer((HELLO + v).getBytes(StandardCharsets.UTF_8)))
                            .contentType("text/plain");
                    return Message.of(builder.build());
                })
                .subscribe((Flow.Subscriber<? super Message<?>>) sink);

        assertThat(msgsReceived.await(6, TimeUnit.SECONDS)).isTrue();

        AtomicInteger count = new AtomicInteger();
        messagesReceived.forEach(msg -> {
            assertThat(msg.getContentType()).isEqualTo("text/plain");
            assertThat(msg.getSubject()).isEqualTo("baz");

            Section body = msg.getBody();
            assertThat(body).isInstanceOf(Data.class);
            byte[] receievedBytes = Binary.copy(((Data) body).getValue()).getArray();

            byte[] expectedBytes = (HELLO + count.get()).getBytes(StandardCharsets.UTF_8);
            assertThat(receievedBytes).isEqualTo(expectedBytes);

            count.incrementAndGet();
        });

        assertThat(count.get()).isEqualTo(msgCount);
    }

    @Test
    @Timeout(30)
    @SuppressWarnings({ "deprecation" })
    public void testSinkUsingAmqpMessageAndChannelNameProperty() throws Exception {
        int msgCount = 10;
        String topic = UUID.randomUUID().toString();
        CountDownLatch msgsReceived = new CountDownLatch(msgCount);
        List<org.apache.qpid.proton.message.Message> messagesReceived = Collections
                .synchronizedList(new ArrayList<>(msgCount));
        AtomicReference<String> attachAddress = new AtomicReference<>("non-null-initialisation-value");

        server = setupMockServerForTypeTest(messagesReceived, msgsReceived, attachAddress);

        Flow.Subscriber<? extends Message<?>> sink = createProviderAndSinkUsingChannelName(topic,
                server.actualPort());

        Multi.createFrom().range(0, 10)
                .map(v -> AmqpMessage.<String> builder().withBody(HELLO + v).withSubject("foo").build())
                .subscribe((Flow.Subscriber<? super AmqpMessage<String>>) sink);

        assertThat(msgsReceived.await(6, TimeUnit.SECONDS)).isTrue();

        AtomicInteger count = new AtomicInteger();
        messagesReceived.forEach(msg -> {
            assertThat(msg.getAddress()).isEqualTo(topic);
            assertThat(msg.getSubject()).isEqualTo("foo");

            Section body = msg.getBody();
            assertThat(body).isInstanceOf(AmqpValue.class);
            Object payload = ((AmqpValue) body).getValue();

            assertThat(HELLO + count).isEqualTo(payload);
            count.incrementAndGet();
        });

        assertThat(count.get()).isEqualTo(msgCount);

        // Should have used an anonymous sender link, verify null target address
        assertThat(attachAddress.get()).isNull();
    }

    @SuppressWarnings("unchecked")
    @Test
    @Timeout(30)
    public void testOutgoingMetadata() throws Exception {
        String address = UUID.randomUUID().toString();
        int msgCount = 10;
        CountDownLatch msgsReceived = new CountDownLatch(msgCount);
        List<org.apache.qpid.proton.message.Message> messagesReceived = Collections
                .synchronizedList(new ArrayList<>(msgCount));

        server = setupMockServerForTypeTest(messagesReceived, msgsReceived);

        Flow.Subscriber<? extends Message<?>> sink = createProviderAndSink(address, server.actualPort());

        //noinspection unchecked
        Multi.createFrom().range(0, 10)
                .map(Message::of)
                .map(m -> m.addMetadata(OutgoingAmqpMetadata.builder()
                        .withSubject("subject")
                        .withMessageId("my-id")
                        .withReplyTo("reply-to")
                        .withReplyToGroupId("reply-to-group")
                        .withPriority((short) 6)
                        .withTtl(2000)
                        .withGroupId("group")
                        .withContentType("text/plain")
                        .withCorrelationId("correlation-" + m.getPayload())
                        .withUserId("user")
                        .withDeliveryAnnotations("some-delivery-annotation", "da-value")
                        .withMessageAnnotations("some-msg-annotation", "ma-value")
                        .withApplicationProperties(new JsonObject().put("key", "value"))
                        .withFooter("my-trailer", "hello-footer")
                        .build()))
                .subscribe((Flow.Subscriber<? super Message<Integer>>) sink);

        assertThat(msgsReceived.await(6, TimeUnit.SECONDS)).isTrue();

        AtomicInteger count = new AtomicInteger();
        messagesReceived.forEach(msg -> {
            assertThat(msg.getAddress()).isEqualTo(address);
            assertThat(msg.getSubject()).isEqualTo("subject");
            assertThat(msg.getMessageId()).isEqualTo("my-id");
            assertThat(msg.getReplyTo()).isEqualTo("reply-to");
            assertThat(msg.getReplyToGroupId()).isEqualTo("reply-to-group");
            assertThat(msg.getPriority()).isEqualTo((short) 6);
            assertThat(msg.getTtl()).isEqualTo(2000);
            assertThat(msg.getGroupId()).isEqualTo("group");
            assertThat(msg.getContentType()).isEqualTo("text/plain");
            assertThat(msg.getCorrelationId()).isEqualTo("correlation-" + count.get());
            assertThat(msg.isFirstAcquirer()).isFalse();
            assertThat(msg.getUserId()).isEqualTo("user".getBytes(StandardCharsets.UTF_8));

            assertThat(msg.getDeliveryAnnotations()).isNotNull();
            assertThat(msg.getDeliveryAnnotations().getValue())
                    .containsExactly(entry(Symbol.valueOf("some-delivery-annotation"), "da-value"));

            assertThat(msg.getMessageAnnotations()).isNotNull();
            assertThat(msg.getMessageAnnotations().getValue())
                    .containsExactly(entry(Symbol.valueOf("some-msg-annotation"), "ma-value"));

            assertThat(msg.getApplicationProperties()).isNotNull();
            assertThat(msg.getApplicationProperties().getValue()).containsExactly(entry("key", "value"));

            assertThat(msg.getFooter()).isNotNull();
            //noinspection unchecked
            assertThat(msg.getFooter().getValue()).containsExactly(entry("my-trailer", "hello-footer"));

            Section body = msg.getBody();
            assertThat(body).isInstanceOf(AmqpValue.class);
            assertThat(((AmqpValue) body).getValue()).isEqualTo(count.get());

            count.incrementAndGet();
        });

        assertThat(count.get()).isEqualTo(msgCount);
    }

    @SuppressWarnings("unchecked")
    @Test
    @Timeout(30)
    public void testOutgoingMetadataWithTtlSetOnConnector() throws Exception {
        String address = UUID.randomUUID().toString();
        int msgCount = 10;
        CountDownLatch msgsReceived = new CountDownLatch(msgCount);
        List<org.apache.qpid.proton.message.Message> messagesReceived = Collections
                .synchronizedList(new ArrayList<>(msgCount));

        server = setupMockServerForTypeTest(messagesReceived, msgsReceived);

        Flow.Subscriber<? extends Message<?>> sink = createProviderAndSinkWithConnectorTtl(address,
                server.actualPort(),
                3000);

        //noinspection unchecked
        Multi.createFrom().range(0, 10)
                .map(Message::of)
                .map(m -> m.addMetadata(OutgoingAmqpMetadata.builder()
                        .withSubject("subject")
                        .withMessageId("my-id")
                        .withReplyTo("reply-to")
                        .withReplyToGroupId("reply-to-group")
                        .withPriority((short) 6)
                        .withGroupId("group")
                        .withContentType("text/plain")
                        .withCorrelationId("correlation-" + m.getPayload())
                        .withUserId("user")
                        .withDeliveryAnnotations("some-delivery-annotation", "da-value")
                        .withMessageAnnotations("some-msg-annotation", "ma-value")
                        .withApplicationProperties(new JsonObject().put("key", "value"))
                        .withFooter("my-trailer", "hello-footer")
                        .build()))
                .subscribe((Flow.Subscriber<? super Message<Integer>>) sink);

        assertThat(msgsReceived.await(6, TimeUnit.SECONDS)).isTrue();

        AtomicInteger count = new AtomicInteger();
        messagesReceived.forEach(msg -> {
            assertThat(msg.getAddress()).isEqualTo(address);
            assertThat(msg.getSubject()).isEqualTo("subject");
            assertThat(msg.getMessageId()).isEqualTo("my-id");
            assertThat(msg.getReplyTo()).isEqualTo("reply-to");
            assertThat(msg.getReplyToGroupId()).isEqualTo("reply-to-group");
            assertThat(msg.getPriority()).isEqualTo((short) 6);
            assertThat(msg.getTtl()).isEqualTo(3000);
            assertThat(msg.getGroupId()).isEqualTo("group");
            assertThat(msg.getContentType()).isEqualTo("text/plain");
            assertThat(msg.getCorrelationId()).isEqualTo("correlation-" + count.get());
            assertThat(msg.isFirstAcquirer()).isFalse();
            assertThat(msg.getUserId()).isEqualTo("user".getBytes(StandardCharsets.UTF_8));

            assertThat(msg.getDeliveryAnnotations()).isNotNull();
            assertThat(msg.getDeliveryAnnotations().getValue())
                    .containsExactly(entry(Symbol.valueOf("some-delivery-annotation"), "da-value"));

            assertThat(msg.getMessageAnnotations()).isNotNull();
            assertThat(msg.getMessageAnnotations().getValue())
                    .containsExactly(entry(Symbol.valueOf("some-msg-annotation"), "ma-value"));

            assertThat(msg.getApplicationProperties()).isNotNull();
            assertThat(msg.getApplicationProperties().getValue()).containsExactly(entry("key", "value"));

            assertThat(msg.getFooter()).isNotNull();
            //noinspection unchecked
            assertThat(msg.getFooter().getValue()).containsExactly(entry("my-trailer", "hello-footer"));

            Section body = msg.getBody();
            assertThat(body).isInstanceOf(AmqpValue.class);
            assertThat(((AmqpValue) body).getValue()).isEqualTo(count.get());

            count.incrementAndGet();
        });

        assertThat(count.get()).isEqualTo(msgCount);
    }

    @SuppressWarnings("unchecked")
    @Test
    @Timeout(30)
    public void testOutgoingMetadataWithTtlSetOnConnectorButOverriddenInMessage() throws Exception {
        String address = UUID.randomUUID().toString();
        int msgCount = 10;
        CountDownLatch msgsReceived = new CountDownLatch(msgCount);
        List<org.apache.qpid.proton.message.Message> messagesReceived = Collections
                .synchronizedList(new ArrayList<>(msgCount));

        server = setupMockServerForTypeTest(messagesReceived, msgsReceived);

        Flow.Subscriber<? extends Message<?>> sink = createProviderAndSinkWithConnectorTtl(address,
                server.actualPort(),
                3000);

        //noinspection unchecked
        Multi.createFrom().range(0, 10)
                .map(Message::of)
                .map(m -> m.addMetadata(OutgoingAmqpMetadata.builder()
                        .withSubject("subject")
                        .withMessageId("my-id")
                        .withReplyTo("reply-to")
                        .withReplyToGroupId("reply-to-group")
                        .withPriority((short) 6)
                        .withTtl(4000)
                        .withGroupId("group")
                        .withContentType("text/plain")
                        .withCorrelationId("correlation-" + m.getPayload())
                        .withUserId("user")
                        .withDeliveryAnnotations("some-delivery-annotation", "da-value")
                        .withMessageAnnotations("some-msg-annotation", "ma-value")
                        .withApplicationProperties(new JsonObject().put("key", "value"))
                        .withFooter("my-trailer", "hello-footer")
                        .build()))
                .subscribe((Flow.Subscriber<? super Message<Integer>>) sink);

        assertThat(msgsReceived.await(6, TimeUnit.SECONDS)).isTrue();

        AtomicInteger count = new AtomicInteger();
        messagesReceived.forEach(msg -> {
            assertThat(msg.getAddress()).isEqualTo(address);
            assertThat(msg.getSubject()).isEqualTo("subject");
            assertThat(msg.getMessageId()).isEqualTo("my-id");
            assertThat(msg.getReplyTo()).isEqualTo("reply-to");
            assertThat(msg.getReplyToGroupId()).isEqualTo("reply-to-group");
            assertThat(msg.getPriority()).isEqualTo((short) 6);
            assertThat(msg.getTtl()).isEqualTo(4000);
            assertThat(msg.getGroupId()).isEqualTo("group");
            assertThat(msg.getContentType()).isEqualTo("text/plain");
            assertThat(msg.getCorrelationId()).isEqualTo("correlation-" + count.get());
            assertThat(msg.isFirstAcquirer()).isFalse();
            assertThat(msg.getUserId()).isEqualTo("user".getBytes(StandardCharsets.UTF_8));

            assertThat(msg.getDeliveryAnnotations()).isNotNull();
            assertThat(msg.getDeliveryAnnotations().getValue())
                    .containsExactly(entry(Symbol.valueOf("some-delivery-annotation"), "da-value"));

            assertThat(msg.getMessageAnnotations()).isNotNull();
            assertThat(msg.getMessageAnnotations().getValue())
                    .containsExactly(entry(Symbol.valueOf("some-msg-annotation"), "ma-value"));

            assertThat(msg.getApplicationProperties()).isNotNull();
            assertThat(msg.getApplicationProperties().getValue()).containsExactly(entry("key", "value"));

            assertThat(msg.getFooter()).isNotNull();
            //noinspection unchecked
            assertThat(msg.getFooter().getValue()).containsExactly(entry("my-trailer", "hello-footer"));

            Section body = msg.getBody();
            assertThat(body).isInstanceOf(AmqpValue.class);
            assertThat(((AmqpValue) body).getValue()).isEqualTo(count.get());

            count.incrementAndGet();
        });

        assertThat(count.get()).isEqualTo(msgCount);
    }

    @SuppressWarnings("unchecked")
    @Test
    @Timeout(30)
    public void testUnusedMessagesSectionsNotSent() throws Exception {
        String address = UUID.randomUUID().toString();
        int msgCount = 10;
        CountDownLatch msgsReceived = new CountDownLatch(msgCount);
        List<org.apache.qpid.proton.message.Message> messagesReceived = Collections
                .synchronizedList(new ArrayList<>(msgCount));

        server = setupMockServerForTypeTest(messagesReceived, msgsReceived);

        Flow.Subscriber<? extends Message<?>> sink = createProviderAndSink(address, server.actualPort());

        //noinspection unchecked
        Multi.createFrom().range(0, 10)
                .map(Message::of)
                .subscribe((Flow.Subscriber<? super Message<Integer>>) sink);

        assertThat(msgsReceived.await(6, TimeUnit.SECONDS)).isTrue();

        AtomicInteger count = new AtomicInteger();
        messagesReceived.forEach(msg -> {
            assertThat(msg.getAddress()).isEqualTo(address);

            Section body = msg.getBody();
            assertThat(body).isInstanceOf(AmqpValue.class);
            assertThat(((AmqpValue) body).getValue()).isEqualTo(count.get());

            // Check that the header, delivery annotations, message annotations, application properties,
            // and footer sections are null/not-present, as we didnt populate anything that goes in them.
            assertThat(msg.getHeader()).isNull();
            assertThat(msg.getDeliveryAnnotations()).isNull();
            assertThat(msg.getMessageAnnotations()).isNull();
            assertThat(msg.getApplicationProperties()).isNull();
            assertThat(msg.getFooter()).isNull();

            count.incrementAndGet();
        });

        assertThat(count.get()).isEqualTo(msgCount);
    }

    private Flow.Subscriber<? extends Message<?>> getSubscriberBuilder(Map<String, Object> config) {
        this.provider = new AmqpConnector();
        provider.setup(executionHolder);
        provider.init();
        return this.provider.getSubscriber(new MapBasedConfig(config));
    }

    private Map<String, Object> createBaseConfig(String topic, int port) {
        Map<String, Object> config = new HashMap<>();
        config.put(ConnectorFactory.CHANNEL_NAME_ATTRIBUTE, topic);
        config.put("name", "the name");
        config.put("host", "localhost");
        config.put("port", port);
        config.put("tracing-enabled", false);

        return config;
    }

    private Flow.Subscriber<? extends Message<?>> createProviderAndSink(String topic, int port) {
        Map<String, Object> config = createBaseConfig(topic, port);
        config.put("address", topic);

        return getSubscriberBuilder(config);
    }

    @SuppressWarnings("SameParameterValue")
    private Flow.Subscriber<? extends Message<?>> createProviderAndSinkWithConnectorTtl(String topic, int port,
            long ttl) {
        Map<String, Object> config = createBaseConfig(topic, port);
        config.put("address", topic);
        config.put("ttl", ttl);

        return getSubscriberBuilder(config);
    }

    private Flow.Subscriber<? extends Message<?>> createProviderAndNonAnonymousSink(String topic, int port) {
        Map<String, Object> config = createBaseConfig(topic, port);
        config.put("address", topic);
        config.put("use-anonymous-sender", false);

        return getSubscriberBuilder(config);
    }

    private Flow.Subscriber<? extends Message<?>> createProviderAndSinkUsingChannelName(String topic,
            int port) {
        Map<String, Object> config = createBaseConfig(topic, port);
        // We don't add the address config element, relying on the channel name instead

        return getSubscriberBuilder(config);
    }

}
