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
import org.junit.jupiter.api.Disabled;
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
    @Disabled
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

    @Test
    @Disabled
    public void testDirectAutoRecoveryAfterBrokerRestart() throws JMSException {
        // Use factory-based context creator matching production behavior
        JmsResourceHolder<JMSProducer> holder = new JmsResourceHolder<>("jms",
                () -> factory.createContext());

        MapBasedConfig config = new MapBasedConfig()
                .with("destination", "queue-one")
                .with("channel-name", "jms")
                .with("retry", true)
                .with("retry.initial-delay", "PT0.1S")
                .with("retry.max-delay", "PT1S")
                .with("retry.max-retries", 10)
                .with("retry.jitter", 0.0);

        JmsSink sink = new JmsSink(holder, new JmsConnectorOutgoingConfiguration(config),
                UnsatisfiedInstance.instance(),
                jsonMapping,
                executor);

        MyJmsClient client = new MyJmsClient(jms.createQueue("queue-one"));
        subscriber = sink.getSink();
        subscriber.onSubscribe(new Subscriptions.EmptySubscription());

        // Send message, verify receipt
        AtomicBoolean acked1 = new AtomicBoolean();
        subscriber.onNext(Message.of("first",
                () -> CompletableFuture.runAsync(() -> acked1.set(true))));
        await().until(() -> client.messages.size() >= 1);
        assertThat(acked1).isTrue();
        assertThat(client.messages.get(0).getBody(String.class)).isEqualTo("first");

        // Close the test consumer's JMS context before stopping broker
        jms.close();

        // Stop and restart broker - breaks existing connections
        stopArtemis();
        restartArtemis();

        // Close the resource holder - simulates what the onFailure handler does on send failure.
        // This nullifies context/producer/destination so lazy getters recreate them.
        holder.close();

        // Reinitialize the factory and JMS context for the test consumer
        // Note: don't call init()/close() as they manage the executor which the sink still uses
        factory = new ActiveMQJMSConnectionFactory("tcp://localhost:61616", null, null);
        jms = factory.createContext();
        MyJmsClient client2 = new MyJmsClient(jms.createQueue("queue-one"));

        // Send a message after broker restart and holder close.
        // The holder lazily recreates the context, producer and destination from the factory.
        AtomicBoolean acked2 = new AtomicBoolean();
        subscriber.onNext(Message.of("after-recovery",
                () -> CompletableFuture.runAsync(() -> acked2.set(true))));

        await().atMost(10, TimeUnit.SECONDS).until(acked2::get);
        await().until(() -> client2.messages.size() >= 1);
        assertThat(client2.messages.get(0).getBody(String.class)).isEqualTo("after-recovery");
    }

    @Test
    @Disabled
    public void testDirectNoRecoveryWhenRetryDisabled() throws JMSException, InterruptedException {
        // Use factory-based context creator matching production behavior
        JmsResourceHolder<JMSProducer> holder = new JmsResourceHolder<>("jms",
                () -> factory.createContext());

        MapBasedConfig config = new MapBasedConfig()
                .with("destination", "queue-one")
                .with("channel-name", "jms")
                .with("retry", false);

        JmsSink sink = new JmsSink(holder, new JmsConnectorOutgoingConfiguration(config),
                UnsatisfiedInstance.instance(),
                jsonMapping,
                executor);

        MyJmsClient client = new MyJmsClient(jms.createQueue("queue-one"));
        subscriber = sink.getSink();
        subscriber.onSubscribe(new Subscriptions.EmptySubscription());

        // Send message successfully
        AtomicBoolean acked1 = new AtomicBoolean();
        subscriber.onNext(Message.of("before-disconnect",
                () -> CompletableFuture.runAsync(() -> acked1.set(true))));
        await().until(() -> client.messages.size() >= 1);
        assertThat(acked1).isTrue();
        assertThat(client.messages.get(0).getBody(String.class)).isEqualTo("before-disconnect");

        // Close the test consumer's JMS context before stopping broker
        jms.close();

        // Stop broker - the JMS context/producer in the holder become broken
        stopArtemis();

        // Wait for broker to fully stop and disconnect to be detected
        Thread.sleep(1000);

        // Send a message while broker is down - the send will fail in dispatch.
        // With retry=false, the failure terminates the stream.
        AtomicBoolean acked2 = new AtomicBoolean();
        subscriber.onNext(Message.of("during-outage",
                () -> CompletableFuture.runAsync(() -> acked2.set(true))));

        // Wait for the failure to propagate and terminate the stream
        await().pollDelay(3, TimeUnit.SECONDS).until(() -> true);
        assertThat(acked2).isFalse();

        // Restart broker
        restartArtemis();
        factory = new ActiveMQJMSConnectionFactory("tcp://localhost:61616", null, null);
        jms = factory.createContext();
        MyJmsClient client2 = new MyJmsClient(jms.createQueue("queue-one"));

        // Send another message after restart - should NOT be processed because stream is terminated
        AtomicBoolean acked3 = new AtomicBoolean();
        subscriber.onNext(Message.of("after-restart",
                () -> CompletableFuture.runAsync(() -> acked3.set(true))));

        // Verify the message is NOT delivered (stream was terminally failed)
        await().pollDelay(3, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    assertThat(acked3).isFalse();
                    assertThat(client2.messages).isEmpty();
                });
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
