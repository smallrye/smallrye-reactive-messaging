package io.smallrye.reactive.messaging.jms;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.Topic;
import javax.json.bind.Jsonb;
import javax.json.bind.JsonbBuilder;

import org.apache.activemq.artemis.jms.client.ActiveMQJMSConnectionFactory;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.CompletionSubscriber;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.smallrye.mutiny.helpers.Subscriptions;
import io.smallrye.reactive.messaging.jms.support.JmsTestBase;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class JmsSinkTest extends JmsTestBase {

    private JMSContext jms;
    private ActiveMQJMSConnectionFactory factory;
    private Jsonb json;
    private ExecutorService executor;
    private CompletionSubscriber<org.eclipse.microprofile.reactive.messaging.Message<?>, Void> subscriber;

    @Before
    public void init() {
        factory = new ActiveMQJMSConnectionFactory(
                "tcp://localhost:61616",
                null, null);
        jms = factory.createContext();
        json = JsonbBuilder.create();
        executor = Executors.newFixedThreadPool(3);
    }

    @After
    public void close() throws Exception {
        jms.close();
        factory.close();
        json.close();
        executor.shutdown();
    }

    @Test
    public void testDefaultConfiguration() throws JMSException {
        MapBasedConfig config = new MapBasedConfig()
                .put("destination", "queue-one")
                .put("channel-name", "jms");
        JmsSink sink = new JmsSink(jms, new JmsConnectorOutgoingConfiguration(config), json, executor);
        MyJmsClient client = new MyJmsClient(jms.createQueue("queue-one"));
        subscriber = sink.getSink().build();
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
                .put("destination", "my-topic")
                .put("destination-type", "topic")
                .put("channel-name", "jms");
        JmsSink sink = new JmsSink(jms, new JmsConnectorOutgoingConfiguration(config), json, executor);
        MyJmsClient client1 = new MyJmsClient(jms.createTopic("my-topic"));
        MyJmsClient client2 = new MyJmsClient(jms.createTopic("my-topic"));
        subscriber = sink.getSink().build();
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
                .put("destination", "queue-one")
                .put("delivery-delay", 1500L)
                .put("delivery-mode", "non_persistent")
                .put("channel-name", "jms");
        JmsSink sink = new JmsSink(jms, new JmsConnectorOutgoingConfiguration(config), json, executor);
        MyJmsClient client = new MyJmsClient(jms.createQueue("queue-one"));
        subscriber = sink.getSink().build();
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
                .put("destination", "queue-one")
                .put("disable-message-id", true)
                .put("disable-message-timestamp", true)
                .put("channel-name", "jms");
        JmsSink sink = new JmsSink(jms, new JmsConnectorOutgoingConfiguration(config), json, executor);
        MyJmsClient client = new MyJmsClient(jms.createQueue("queue-one"));
        subscriber = sink.getSink().build();
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
                .put("destination", "queue-one")
                .put("correlation-id", "my-correlation")
                .put("priority", 5)
                .put("ttl", 1000L)
                .put("channel-name", "jms");
        JmsSink sink = new JmsSink(jms, new JmsConnectorOutgoingConfiguration(config), json, executor);
        MyJmsClient client = new MyJmsClient(jms.createQueue("queue-one"));
        subscriber = sink.getSink().build();
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
                .put("destination", "queue-one")
                .put("reply-to", "my-response")
                .put("channel-name", "jms");
        JmsSink sink = new JmsSink(jms, new JmsConnectorOutgoingConfiguration(config), json, executor);
        MyJmsClient client = new MyJmsClient(jms.createQueue("queue-one"));
        subscriber = sink.getSink().build();
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

    @Test
    public void testWithReplyToTopic() throws JMSException {
        MapBasedConfig config = new MapBasedConfig()
                .put("destination", "queue-one")
                .put("reply-to", "my-response")
                .put("reply-to-destination-type", "topic")
                .put("channel-name", "jms");
        JmsSink sink = new JmsSink(jms, new JmsConnectorOutgoingConfiguration(config), json, executor);
        MyJmsClient client = new MyJmsClient(jms.createQueue("queue-one"));
        subscriber = sink.getSink().build();
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

    @Test(expected = IllegalArgumentException.class)
    public void testWithReplyToWithInvalidDestinationType() {
        MapBasedConfig config = new MapBasedConfig()
                .put("destination", "queue-one")
                .put("reply-to", "my-response")
                .put("reply-to-destination-type", "invalid")
                .put("channel-name", "jms");
        new JmsSink(jms, new JmsConnectorOutgoingConfiguration(config), json, executor);
    }

    @Test
    public void testPropagation() throws JMSException {
        MapBasedConfig config = new MapBasedConfig()
                .put("destination", "queue-one")
                .put("channel-name", "jms")
                .put("ttl", 10000L);
        JmsSink sink = new JmsSink(jms, new JmsConnectorOutgoingConfiguration(config), json, executor);
        MyJmsClient client = new MyJmsClient(jms.createQueue("queue-one"));
        subscriber = sink.getSink().build();
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
        javax.jms.Message message = client.messages.get(0);
        assertThat(message.getBody(String.class)).isEqualTo("hello");
        assertThat(message.getJMSCorrelationID()).isEqualTo("my-correlation-id");
        assertThat(message.getJMSReplyTo()).isEqualTo(rt);
        assertThat(message.getJMSDeliveryMode()).isEqualTo(2);
        assertThat(message.getJMSType()).isEqualTo(String.class.getName());

    }

    private class MyJmsClient {

        private final List<javax.jms.Message> messages = new CopyOnWriteArrayList<>();

        MyJmsClient(Destination destination) {
            JMSConsumer consumer = jms.createConsumer(destination);
            consumer.setMessageListener(messages::add);
        }
    }

}
