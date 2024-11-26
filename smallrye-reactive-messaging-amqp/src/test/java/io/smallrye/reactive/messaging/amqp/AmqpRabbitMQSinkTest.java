
package io.smallrye.reactive.messaging.amqp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.amqp.AmqpReceiverOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.buffer.Buffer;

public class AmqpRabbitMQSinkTest extends RabbitMQBrokerTestBase {

    private static final String FOO = "foo";
    private static final String ID = "id";
    private static final String HELLO = "hello-";
    private AmqpConnector provider;

    @AfterEach
    public void cleanup() {
        if (provider != null) {
            provider.terminate(null);
        }

        MapBasedConfig.cleanup();
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
    }

    @Test
    public void testSinkUsingIntegerUsingDefaultAnonymousSender() {
        int msgCount = 10;
        String topic = UUID.randomUUID().toString();
        List<org.apache.qpid.proton.message.Message> messagesReceived = new CopyOnWriteArrayList<>();
        usage.consume(topic, new AmqpReceiverOptions().setDurable(false),
                msg -> messagesReceived.add(msg.getDelegate().unwrap()));

        Flow.Subscriber<? extends Message<?>> sink = createProviderAndSink(topic);

        //noinspection unchecked
        Multi.createFrom().range(0, msgCount)
                .map(Message::of)
                .subscribe((Flow.Subscriber<? super Message<Integer>>) sink);

        await().until(() -> messagesReceived.size() == msgCount);

        assertThat(messagesReceived).allSatisfy(msg -> {
            assertThat(msg.getBody()).isInstanceOf(AmqpValue.class);
            assertThat(msg.getAddress()).isEqualTo(topic);
        }).extracting(msg -> ((AmqpValue) msg.getBody()).getValue())
                .containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    }

    @Test
    public void testSinkWhenConnectionIsDown() {
        int msgCount = 10;
        String topic = UUID.randomUUID().toString();
        List<org.apache.qpid.proton.message.Message> messagesReceived = new CopyOnWriteArrayList<>();
        usage.consume(topic, new AmqpReceiverOptions().setDurable(false),
                msg -> messagesReceived.add(msg.getDelegate().unwrap()));

        Flow.Subscriber<? extends Message<?>> sink = createProviderAndSinkOrdered(topic);

        //noinspection unchecked
        Multi.createFrom().ticks().every(Duration.ofMillis(300))
                .map(Long::intValue)
                .map(Message::of)
                .onOverflow().buffer()
                .select().first(msgCount)
                .subscribe((Flow.Subscriber<? super Message<Integer>>) sink);

        await().until(() -> messagesReceived.size() > msgCount / 2);

        stopBroker();

        await().pollDelay(2, TimeUnit.SECONDS).until(() -> true);

        restartBroker();

        await().untilAsserted(() -> assertThat(messagesReceived).hasSizeGreaterThanOrEqualTo(msgCount));

        assertThat(messagesReceived).allSatisfy(msg -> {
            assertThat(msg.getBody()).isInstanceOf(AmqpValue.class);
            assertThat(msg.getAddress()).isEqualTo(topic);
        }).extracting(msg -> ((AmqpValue) msg.getBody()).getValue())
                .containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    }

    @Test
    public void testSinkUsingIntegerUsingNonAnonymousSender() {
        int msgCount = 10;
        String topic = UUID.randomUUID().toString();
        List<org.apache.qpid.proton.message.Message> messagesReceived = new CopyOnWriteArrayList<>();
        usage.consume(topic, new AmqpReceiverOptions().setDurable(false),
                msg -> messagesReceived.add(msg.getDelegate().unwrap()));

        Flow.Subscriber<? extends Message<?>> sink = createProviderAndNonAnonymousSink(topic);

        //noinspection unchecked
        Multi.createFrom().range(0, msgCount)
                .map(Message::of)
                .subscribe((Flow.Subscriber<? super Message<Integer>>) sink);

        await().until(() -> messagesReceived.size() == msgCount);

        // Should have used a fixed-address sender link, vertify target address
        assertThat(messagesReceived).allSatisfy(msg -> {
            assertThat(msg.getBody()).isInstanceOf(AmqpValue.class);
            assertThat(msg.getAddress()).isEqualTo(topic);
        }).extracting(msg -> ((AmqpValue) msg.getBody()).getValue())
                .containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    }

    @Test
    public void testSinkUsingString() {
        int msgCount = 10;
        String topic = UUID.randomUUID().toString();
        List<org.apache.qpid.proton.message.Message> messagesReceived = new CopyOnWriteArrayList<>();
        usage.consume(topic, new AmqpReceiverOptions().setDurable(false),
                msg -> messagesReceived.add(msg.getDelegate().unwrap()));

        Flow.Subscriber<? extends Message<?>> sink = createProviderAndNonAnonymousSink(topic);
        //noinspection unchecked
        Multi.createFrom().range(0, 10)
                .map(i -> Integer.toString(i))
                .map(Message::of)
                .subscribe((Flow.Subscriber<? super Message<String>>) sink);

        await().until(() -> messagesReceived.size() == msgCount);

        assertThat(messagesReceived).allSatisfy(msg -> {
            assertThat(msg.getBody()).isInstanceOf(AmqpValue.class);
            assertThat(msg.getAddress()).isEqualTo(topic);
        }).extracting(msg -> ((AmqpValue) msg.getBody()).getValue())
                .containsExactly("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
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
        int msgCount = 10;
        String topic = UUID.randomUUID().toString();
        List<org.apache.qpid.proton.message.Message> messagesReceived = new CopyOnWriteArrayList<>();
        usage.consume(topic, new AmqpReceiverOptions().setDurable(false),
                msg -> messagesReceived.add(msg.getDelegate().unwrap()));

        Flow.Subscriber<? extends Message<?>> sink = createProviderAndSink(topic);
        //noinspection unchecked
        Multi.createFrom().range(0, 10)
                .map(i -> {
                    Person p = new Person();
                    p.setName(HELLO + i);
                    return p;
                })
                .map(Message::of)
                .subscribe((Flow.Subscriber<? super Message<Person>>) sink);

        await().untilAsserted(() -> assertThat(messagesReceived).hasSize(msgCount));

        assertThat(messagesReceived).allSatisfy(msg -> {
            assertThat(msg.getContentType()).isEqualTo("application/json");
            Section body = msg.getBody();
            assertThat(body).isInstanceOf(Data.class);
        }).extracting(msg -> {
            Section body = msg.getBody();
            Binary bin = ((Data) body).getValue();
            byte[] bytes = Binary.copy(bin).getArray();
            return Buffer.buffer(bytes).toJsonObject().mapTo(Person.class);
        }).extracting(Person::getName)
                .containsExactly("hello-0", "hello-1", "hello-2", "hello-3", "hello-4", "hello-5", "hello-6", "hello-7",
                        "hello-8", "hello-9");
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
    public void testSinkUsingObjectThatCannotBeSerialized() {
        int msgCount = 10;
        String topic = UUID.randomUUID().toString();
        AtomicInteger nack = new AtomicInteger();
        List<org.apache.qpid.proton.message.Message> messagesReceived = new CopyOnWriteArrayList<>();
        usage.consume(topic, new AmqpReceiverOptions().setDurable(false),
                msg -> messagesReceived.add(msg.getDelegate().unwrap()));

        Flow.Subscriber<? extends Message<?>> sink = createProviderAndSink(topic);
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

        await().untilAsserted(() -> assertThat(messagesReceived).hasSize(msgCount / 2));
        await().until(() -> nack.get() == 5);

        assertThat(messagesReceived).allSatisfy(msg -> {
            assertThat(msg.getContentType()).isEqualTo("application/json");
            assertThat(msg.getBody()).isInstanceOf(Data.class);
        }).extracting(msg -> {
            Binary bin = ((Data) msg.getBody()).getValue();
            byte[] bytes = Binary.copy(bin).getArray();
            return Buffer.buffer(bytes).toJsonObject().mapTo(Person.class);
        }).extracting(Person::getName)
                .containsExactly("hello-0", "hello-2", "hello-4", "hello-6", "hello-8");
    }

    @Test
    public void testSinkUsingJsonObject() {
        int msgCount = 10;
        String topic = UUID.randomUUID().toString();
        List<org.apache.qpid.proton.message.Message> messagesReceived = new CopyOnWriteArrayList<>();
        usage.consume(topic, new AmqpReceiverOptions().setDurable(false),
                msg -> messagesReceived.add(msg.getDelegate().unwrap()));

        Flow.Subscriber<? extends Message<?>> sink = createProviderAndSink(topic);
        //noinspection unchecked
        Multi.createFrom().range(0, 10)
                .map(i -> new JsonObject().put(ID, HELLO + i))
                .map(Message::of)
                .subscribe((Flow.Subscriber<? super Message<?>>) sink);

        await().untilAsserted(() -> assertThat(messagesReceived).hasSize(msgCount));

        assertThat(messagesReceived).allSatisfy(msg -> {
            assertThat(msg.getContentType()).isEqualTo("application/json");
            assertThat(msg.getBody()).isInstanceOf(Data.class);
        }).extracting(msg -> {
            Binary bin = ((Data) msg.getBody()).getValue();
            byte[] bytes = Binary.copy(bin).getArray();
            return Buffer.buffer(bytes).toJsonObject();
        });
    }

    @Test
    public void testSinkUsingJsonArray() {
        int msgCount = 10;
        String topic = UUID.randomUUID().toString();
        List<org.apache.qpid.proton.message.Message> messagesReceived = new CopyOnWriteArrayList<>();
        usage.consume(topic, new AmqpReceiverOptions().setDurable(false),
                msg -> messagesReceived.add(msg.getDelegate().unwrap()));

        Flow.Subscriber<? extends Message<?>> sink = createProviderAndSink(topic);
        //noinspection unchecked
        Multi.createFrom().range(0, 10)
                .map(i -> new JsonArray().add(HELLO + i).add(FOO))
                .map(Message::of)
                .subscribe((Flow.Subscriber<? super Message<?>>) sink);

        await().untilAsserted(() -> assertThat(messagesReceived).hasSize(msgCount));

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
    public void testSinkUsingVertxBuffer() {
        int msgCount = 10;
        String topic = UUID.randomUUID().toString();
        List<org.apache.qpid.proton.message.Message> messagesReceived = new CopyOnWriteArrayList<>();
        usage.consume(topic, new AmqpReceiverOptions().setDurable(false),
                msg -> messagesReceived.add(msg.getDelegate().unwrap()));

        Flow.Subscriber<? extends Message<?>> sink = createProviderAndSink(topic);
        //noinspection unchecked
        Multi.createFrom().range(0, 10)
                .map(i -> new JsonObject().put(ID, HELLO + i).toBuffer())
                .map(Message::of)
                .subscribe((Flow.Subscriber<? super Message<?>>) sink);

        await().untilAsserted(() -> assertThat(messagesReceived).hasSize(msgCount));

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
    public void testSinkUsingMutinyBuffer() {
        int msgCount = 10;
        String topic = UUID.randomUUID().toString();
        List<org.apache.qpid.proton.message.Message> messagesReceived = new CopyOnWriteArrayList<>();
        usage.consume(topic, new AmqpReceiverOptions().setDurable(false),
                msg -> messagesReceived.add(msg.getDelegate().unwrap()));

        Flow.Subscriber<? extends Message<?>> sink = createProviderAndSink(topic);
        //noinspection unchecked
        Multi.createFrom().range(0, 10)
                .map(i -> new Buffer(new JsonObject().put(ID, HELLO + i).toBuffer()))
                .map(Message::of)
                .subscribe((Flow.Subscriber<? super Message<?>>) sink);

        await().untilAsserted(() -> assertThat(messagesReceived).hasSize(msgCount));

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
    public void testSinkUsingByteArray() {
        int msgCount = 10;
        String topic = UUID.randomUUID().toString();
        List<org.apache.qpid.proton.message.Message> messagesReceived = new CopyOnWriteArrayList<>();
        usage.consume(topic, new AmqpReceiverOptions().setDurable(false),
                msg -> messagesReceived.add(msg.getDelegate().unwrap()));

        Flow.Subscriber<? extends Message<?>> sink = createProviderAndSink(topic);
        //noinspection unchecked
        Multi.createFrom().range(0, 10)
                .map(i -> new JsonObject().put(ID, HELLO + i).toBuffer().getBytes())
                .map(Message::of)
                .subscribe((Flow.Subscriber<? super Message<?>>) sink);

        await().untilAsserted(() -> assertThat(messagesReceived).hasSize(msgCount));

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

    private Flow.Subscriber<? extends Message<?>> getSubscriberBuilder(Map<String, Object> config) {
        this.provider = new AmqpConnector();
        provider.setup(executionHolder);
        return this.provider.getSubscriber(new MapBasedConfig(config));
    }

    private Map<String, Object> createBaseConfig(String topic) {
        Map<String, Object> config = new HashMap<>();
        config.put(ConnectorFactory.CHANNEL_NAME_ATTRIBUTE, topic);
        config.put("name", "the name");
        config.put("host", host);
        config.put("port", port);
        config.put("tracing-enabled", false);

        return config;
    }

    private Flow.Subscriber<? extends Message<?>> createProviderAndSink(String topic) {
        Map<String, Object> config = createBaseConfig(topic);
        config.put("address", topic);

        return getSubscriberBuilder(config);
    }

    private Flow.Subscriber<? extends Message<?>> createProviderAndSinkOrdered(String topic) {
        Map<String, Object> config = createBaseConfig(topic);
        config.put("address", topic);
        config.put("max-inflight-messages", 1L);

        return getSubscriberBuilder(config);
    }

    private Flow.Subscriber<? extends Message<?>> createProviderAndNonAnonymousSink(String topic) {
        Map<String, Object> config = createBaseConfig(topic);
        config.put("address", topic);
        config.put("use-anonymous-sender", false);

        return getSubscriberBuilder(config);
    }

}
