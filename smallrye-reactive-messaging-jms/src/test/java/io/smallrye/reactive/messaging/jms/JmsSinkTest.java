package io.smallrye.reactive.messaging.jms;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.jms.*;

import org.apache.activemq.artemis.jms.client.ActiveMQJMSConnectionFactory;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.helpers.Subscriptions;
import io.smallrye.reactive.messaging.json.JsonMapping;
import io.smallrye.reactive.messaging.support.JmsTestBase;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

@SuppressWarnings("ConstantConditions")
public class JmsSinkTest extends JmsTestBase {

    private JMSContext jms;
    private ActiveMQJMSConnectionFactory factory;
    private JsonMapping jsonMapping;
    private ExecutorService executor;
    private Flow.Subscriber<org.eclipse.microprofile.reactive.messaging.Message<?>> subscriber;

    @BeforeEach
    public void init() {
        factory = new ActiveMQJMSConnectionFactory(
                "tcp://localhost:61616",
                null, null);
        jms = factory.createContext();
        jsonMapping = new TestMapping();
        executor = Executors.newFixedThreadPool(3);
    }

    @AfterEach
    public void close() {
        jms.close();
        factory.close();
        executor.shutdown();
    }

    private JmsResourceHolder<JMSProducer> getResourceHolder() {
        return new JmsResourceHolder<>("jms", () -> jms);
    }

    @Test
    public void testDefaultConfiguration() throws JMSException {
        MapBasedConfig config = new MapBasedConfig()
                .with("destination", "queue-one")
                .with("channel-name", "jms");
        JmsSink sink = new JmsSink(getResourceHolder(), new JmsConnectorOutgoingConfiguration(config),
                UnsatisfiedInstance.instance(),
                jsonMapping,
                executor);
        MyJmsClient client = new MyJmsClient(jms.createQueue("queue-one"));
        subscriber = sink.getSink();
        subscriber.onSubscribe(new Subscriptions.EmptySubscription());
        AtomicBoolean acked = new AtomicBoolean();
        subscriber.onNext(Message.of("hello",
                () -> CompletableFuture.runAsync(() -> acked.set(true))));

        await().until(() -> client.messages.size() >= 1);
        assertThat(acked).isTrue();
        assertThat(client.messages.get(0).getBody(String.class)).isEqualTo("hello");
    }

    @Test
    public void testDefaultConfigurationAgainstTopic() throws JMSException {
        MapBasedConfig config = new MapBasedConfig()
                .with("destination", "my-topic")
                .with("destination-type", "topic")
                .with("channel-name", "jms");
        JmsSink sink = new JmsSink(getResourceHolder(), new JmsConnectorOutgoingConfiguration(config),
                UnsatisfiedInstance.instance(),
                jsonMapping,
                executor);
        MyJmsClient client1 = new MyJmsClient(jms.createTopic("my-topic"));
        MyJmsClient client2 = new MyJmsClient(jms.createTopic("my-topic"));
        subscriber = sink.getSink();
        subscriber.onSubscribe(new Subscriptions.EmptySubscription());
        AtomicBoolean acked = new AtomicBoolean();
        subscriber.onNext(Message.of("hello",
                () -> CompletableFuture.runAsync(() -> acked.set(true))));

        await().until(() -> client1.messages.size() >= 1);
        await().until(() -> client2.messages.size() >= 1);
        assertThat(acked).isTrue();
        assertThat(client1.messages.get(0).getBody(String.class)).isEqualTo("hello");
        assertThat(client2.messages.get(0).getBody(String.class)).isEqualTo("hello");
    }

    @Test
    public void testWithDeliveryDelayAndMode() throws JMSException {
        MapBasedConfig config = new MapBasedConfig()
                .with("destination", "queue-one")
                .with("delivery-delay", 1500L)
                .with("delivery-mode", "non_persistent")
                .with("channel-name", "jms");
        JmsSink sink = new JmsSink(getResourceHolder(), new JmsConnectorOutgoingConfiguration(config),
                UnsatisfiedInstance.instance(),
                jsonMapping,
                executor);
        MyJmsClient client = new MyJmsClient(jms.createQueue("queue-one"));
        subscriber = sink.getSink();
        subscriber.onSubscribe(new Subscriptions.EmptySubscription());
        AtomicBoolean acked = new AtomicBoolean();
        subscriber.onNext(Message.of("hello",
                () -> CompletableFuture.runAsync(() -> acked.set(true))));

        await()
                .atLeast(1, TimeUnit.SECONDS)
                .until(() -> client.messages.size() >= 1);
        assertThat(acked).isTrue();
        assertThat(client.messages.get(0).getBody(String.class)).isEqualTo("hello");
        assertThat(client.messages.get(0).getJMSMessageID()).isNotNull();
        assertThat(client.messages.get(0).getJMSTimestamp()).isPositive();
    }

    @Test
    public void testDisablingMessageIdAndTimestamp() throws JMSException {
        MapBasedConfig config = new MapBasedConfig()
                .with("destination", "queue-one")
                .with("disable-message-id", true)
                .with("disable-message-timestamp", true)
                .with("channel-name", "jms");
        JmsSink sink = new JmsSink(getResourceHolder(), new JmsConnectorOutgoingConfiguration(config),
                UnsatisfiedInstance.instance(),
                jsonMapping,
                executor);
        MyJmsClient client = new MyJmsClient(jms.createQueue("queue-one"));
        subscriber = sink.getSink();
        subscriber.onSubscribe(new Subscriptions.EmptySubscription());
        AtomicBoolean acked = new AtomicBoolean();
        subscriber.onNext(Message.of("hello",
                () -> CompletableFuture.runAsync(() -> acked.set(true))));

        await()
                .until(() -> client.messages.size() >= 1);
        assertThat(acked).isTrue();
        assertThat(client.messages.get(0).getBody(String.class)).isEqualTo("hello");
        assertThat(client.messages.get(0).getJMSMessageID()).isNull();
        assertThat(client.messages.get(0).getJMSTimestamp()).isEqualTo(0L);
    }

    @Test
    public void testWithCorrelationIdAndPriorityAndTTL() throws JMSException {
        MapBasedConfig config = new MapBasedConfig()
                .with("destination", "queue-one")
                .with("correlation-id", "my-correlation")
                .with("priority", 5)
                .with("ttl", 1000L)
                .with("channel-name", "jms");
        JmsSink sink = new JmsSink(getResourceHolder(), new JmsConnectorOutgoingConfiguration(config),
                UnsatisfiedInstance.instance(),
                jsonMapping,
                executor);
        MyJmsClient client = new MyJmsClient(jms.createQueue("queue-one"));
        subscriber = sink.getSink();
        subscriber.onSubscribe(new Subscriptions.EmptySubscription());
        AtomicBoolean acked = new AtomicBoolean();
        subscriber.onNext(Message.of("hello",
                () -> CompletableFuture.runAsync(() -> acked.set(true))));

        await()
                .until(() -> client.messages.size() >= 1);
        assertThat(acked).isTrue();
        assertThat(client.messages.get(0).getBody(String.class)).isEqualTo("hello");
        assertThat(client.messages.get(0).getJMSPriority()).isEqualTo(5);
        assertThat(client.messages.get(0).getJMSCorrelationID()).isEqualTo("my-correlation");
    }

    @Test
    public void testWithReplyTo() throws JMSException {
        MapBasedConfig config = new MapBasedConfig()
                .with("destination", "queue-one")
                .with("reply-to", "my-response")
                .with("channel-name", "jms");
        JmsSink sink = new JmsSink(getResourceHolder(), new JmsConnectorOutgoingConfiguration(config),
                UnsatisfiedInstance.instance(),
                jsonMapping,
                executor);
        MyJmsClient client = new MyJmsClient(jms.createQueue("queue-one"));
        subscriber = sink.getSink();
        subscriber.onSubscribe(new Subscriptions.EmptySubscription());
        AtomicBoolean acked = new AtomicBoolean();
        subscriber.onNext(Message.of("hello",
                () -> CompletableFuture.runAsync(() -> acked.set(true))));

        await()
                .until(() -> client.messages.size() >= 1);
        assertThat(acked).isTrue();
        assertThat(client.messages.get(0).getBody(String.class)).isEqualTo("hello");
        assertThat(client.messages.get(0).getJMSReplyTo())
                .isInstanceOf(Queue.class);
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testWithReplyToTopic() throws JMSException {
        MapBasedConfig config = new MapBasedConfig()
                .with("destination", "queue-one")
                .with("reply-to", "my-response")
                .with("reply-to-destination-type", "topic")
                .with("channel-name", "jms");
        JmsSink sink = new JmsSink(getResourceHolder(), new JmsConnectorOutgoingConfiguration(config),
                UnsatisfiedInstance.instance(),
                jsonMapping,
                executor);
        MyJmsClient client = new MyJmsClient(jms.createQueue("queue-one"));
        subscriber = sink.getSink();
        subscriber.onSubscribe(new Subscriptions.EmptySubscription());
        AtomicBoolean acked = new AtomicBoolean();
        subscriber.onNext(Message.of("hello",
                () -> CompletableFuture.runAsync(() -> acked.set(true))));

        await()
                .until(() -> client.messages.size() >= 1);
        assertThat(acked).isTrue();
        assertThat(client.messages.get(0).getBody(String.class)).isEqualTo("hello");
        assertThat(client.messages.get(0).getJMSReplyTo())
                .isInstanceOf(Topic.class);
    }

    @Test
    public void testWithReplyToWithInvalidDestinationType() {
        MapBasedConfig config = new MapBasedConfig()
                .with("destination", "queue-one")
                .with("reply-to", "my-response")
                .with("reply-to-destination-type", "invalid")
                .with("channel-name", "jms");
        assertThatThrownBy(() -> new JmsSink(getResourceHolder(), new JmsConnectorOutgoingConfiguration(config),
                UnsatisfiedInstance.instance(),
                jsonMapping, executor))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ApplicationScoped
    public static class EmitterBean {

        @Inject
        @Channel("jms")
        Emitter<String> jms;

        public void send(String payload) {
            jms.send(payload);
        }
    }

    @Test
    public void testWithDisconnection() {
        Map<String, Object> map = new HashMap<>();
        map.put("mp.messaging.outgoing.jms.connector", JmsConnector.CONNECTOR_NAME);
        map.put("mp.messaging.outgoing.jms.destination", "queue-one");
        MapBasedConfig config = new MapBasedConfig(map);
        addConfig(config);
        WeldContainer container = deploy(EmitterBean.class);

        EmitterBean bean = container.select(EmitterBean.class).get();
        bean.send("1");

        List<jakarta.jms.Message> received = new CopyOnWriteArrayList<>();

        Queue q = jms.createQueue("queue-one");
        JMSConsumer consumer = jms.createConsumer(q);
        consumer.setMessageListener(received::add);

        await().untilAsserted(() -> assertThat(received).hasSize(1)
                .extracting(m -> m.getBody(String.class))
                .containsExactly("1"));

        // close client
        close();

        // send just before stopping
        bean.send("2");
        await().pollDelay(2, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(received).hasSize(1));
        stopArtemis();

        assertThat(received).hasSize(1);

        restartArtemis();

        // send after restart
        bean.send("3");

        // init the client
        init();
        consumer = jms.createConsumer(q);
        consumer.setMessageListener(received::add);

        await().untilAsserted(() -> assertThat(received).hasSizeGreaterThanOrEqualTo(3)
                .extracting(m -> m.getBody(String.class))
                .contains("1", "2", "3")
        // Sometimes the client resends the message because it did not receive the ack from the server.
        // .containsExactly("1", "2", "3")
        );
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testPropagation() throws JMSException {
        MapBasedConfig config = new MapBasedConfig()
                .with("destination", "queue-one")
                .with("channel-name", "jms")
                .with("ttl", 10000L);
        JmsSink sink = new JmsSink(getResourceHolder(), new JmsConnectorOutgoingConfiguration(config),
                UnsatisfiedInstance.instance(),
                jsonMapping,
                executor);
        MyJmsClient client = new MyJmsClient(jms.createQueue("queue-one"));
        subscriber = sink.getSink();
        subscriber.onSubscribe(new Subscriptions.EmptySubscription());
        AtomicBoolean acked = new AtomicBoolean();
        Message<String> hello = Message.of("hello",
                () -> CompletableFuture.runAsync(() -> acked.set(true)));

        Destination rt = jms.createQueue("reply-to");

        OutgoingJmsMessageMetadata metadata = OutgoingJmsMessageMetadata.builder()
                .withCorrelationId("my-correlation-id")
                .withReplyTo(rt)
                .withDeliveryMode(DeliveryMode.PERSISTENT)
                .withType(String.class.getName())
                .build();

        hello = hello.withMetadata(Collections.singleton(metadata));
        subscriber.onNext(hello);

        await().until(() -> client.messages.size() >= 1);
        assertThat(acked).isTrue();
        jakarta.jms.Message message = client.messages.get(0);
        assertThat(message.getBody(String.class)).isEqualTo("hello");
        assertThat(message.getJMSCorrelationID()).isEqualTo("my-correlation-id");
        assertThat(message.getJMSReplyTo()).isEqualTo(rt);
        assertThat(message.getJMSDeliveryMode()).isEqualTo(2);
        assertThat(message.getJMSType()).isEqualTo(String.class.getName());

    }

    private class MyJmsClient {

        private final List<jakarta.jms.Message> messages = new CopyOnWriteArrayList<>();

        MyJmsClient(Destination destination) {
            JMSConsumer consumer = jms.createConsumer(destination);
            consumer.setMessageListener(messages::add);
        }
    }

}
