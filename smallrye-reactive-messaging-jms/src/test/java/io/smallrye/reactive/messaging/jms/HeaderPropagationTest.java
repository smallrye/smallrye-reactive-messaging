package io.smallrye.reactive.messaging.jms;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.awaitility.Awaitility.await;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.JMSProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;

import org.apache.activemq.artemis.jms.client.ActiveMQJMSConnectionFactory;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.jms.support.JmsTestBase;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class HeaderPropagationTest extends JmsTestBase {

    private JMSContext jms;
    private ActiveMQJMSConnectionFactory factory;

    @Before
    public void init() {
        factory = new ActiveMQJMSConnectionFactory(
                "tcp://localhost:61616",
                null, null);
        jms = factory.createContext();
    }

    @After
    public void close() {
        jms.close();
        factory.close();
        // Release the config objects
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
    }

    @Test
    public void testFromAppToJMS() {
        MyJmsClient client = new MyJmsClient(jms.createQueue("some-queue"));
        Map<String, Object> map = new HashMap<>();
        map.put("mp.messaging.outgoing.jms.connector", JmsConnector.CONNECTOR_NAME);
        map.put("mp.messaging.outgoing.jms.destination", "should-not-be-used");
        MapBasedConfig config = new MapBasedConfig(map);
        addConfig(config);
        deploy(MyAppGeneratingData.class);
        await().until(() -> client.messages.size() == 10);
        assertThat(client.messages).allSatisfy(entry -> {
            try {
                assertThat(entry.getBody(String.class)).isNotNull();
                assertThat(entry.getJMSCorrelationID()).startsWith("my-correlation-");
                assertThat(entry.getStringProperty("prop")).isEqualTo("bar");
            } catch (JMSException e) {
                fail("unable to read jms data", e);
            }

        });
    }

    private class MyJmsClient {

        private final List<javax.jms.Message> messages = new CopyOnWriteArrayList<>();

        MyJmsClient(Destination destination) {
            JMSConsumer consumer = jms.createConsumer(destination);
            consumer.setMessageListener(messages::add);
        }
    }

    @Test
    public void testFromJmsToAppToJms() {

        MyJmsClient client = new MyJmsClient(jms.createQueue("some-queue"));
        Map<String, Object> map = new HashMap<>();
        map.put("mp.messaging.incoming.source.connector", JmsConnector.CONNECTOR_NAME);
        map.put("mp.messaging.incoming.source.destination", "source");
        map.put("mp.messaging.outgoing.jms.connector", JmsConnector.CONNECTOR_NAME);
        map.put("mp.messaging.outgoing.jms.destination", "should-not-be-used");
        MapBasedConfig config = new MapBasedConfig(map);
        addConfig(config);
        deploy(MyAppProcessingData.class);

        AtomicInteger count = new AtomicInteger();
        JMSProducer producer = jms.createProducer();
        Queue source = jms.createQueue("source");

        for (int i = 0; i < 20; i++) {
            ObjectMessage message = jms.createObjectMessage(count.getAndIncrement());
            producer.send(source, message);
        }

        await().until(() -> client.messages.size() >= 10);
        assertThat(client.messages).allSatisfy(entry -> {
            try {
                assertThat(entry.getBody(String.class)).isNotNull();
                assertThat(entry.getJMSCorrelationID()).startsWith("my-correlation-");
                assertThat(entry.getStringProperty("prop")).isEqualTo("bar");
            } catch (JMSException e) {
                fail("unable to read jms data", e);
            }
        });
    }

    @ApplicationScoped
    public static class MyAppGeneratingData {

        @Inject
        ConnectionFactory factory;
        private Queue queue;

        @PostConstruct
        public void init() {
            queue = factory.createContext().createQueue("some-queue");
        }

        @Outgoing("source")
        public Multi<Integer> source() {
            return Multi.createFrom().range(0, 10);
        }

        @Incoming("source")
        @Outgoing("p1")
        public Message<Integer> processMessage(Message<Integer> input) {
            JmsProperties properties = new JmsPropertiesBuilder().with("prop", "bar").build();
            return Message.of(input.getPayload())
                    .withMetadata(Metadata.of(OutgoingJmsMessageMetadata.builder()
                            .withProperties(properties)
                            .withCorrelationId("my-correlation-" + input.getPayload())
                            .withDestination(queue)
                            .build()));
        }

        @Incoming("p1")
        @Outgoing("jms")
        public String processPayload(int payload) {
            return Integer.toString(payload);
        }
    }

    @ApplicationScoped
    public static class MyAppProcessingData {

        @Inject
        ConnectionFactory factory;
        private Queue queue;

        @PostConstruct
        public void init() {
            queue = factory.createContext().createQueue("some-queue");
        }

        @Incoming("source")
        @Outgoing("p1")
        public Message<Integer> processMessage(Message<Integer> input) {
            JmsProperties properties = JmsProperties.builder().with("prop", "bar").build();
            return Message.of(input.getPayload())
                    .withMetadata(Metadata.of(OutgoingJmsMessageMetadata.builder()
                            .withProperties(properties)
                            .withCorrelationId("my-correlation-" + input.getPayload())
                            .withDestination(queue)
                            .build()));
        }

        @Incoming("p1")
        @Outgoing("jms")
        public String processPayload(int payload) {
            return Integer.toString(payload);
        }
    }

}
