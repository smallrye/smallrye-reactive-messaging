package io.smallrye.reactive.messaging.jms;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSProducer;
import jakarta.jms.Queue;

import org.apache.activemq.artemis.jms.client.ActiveMQJMSConnectionFactory;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.support.JmsTestBase;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class JmsLocalTransactionTest extends JmsTestBase {

    private ActiveMQJMSConnectionFactory factory;
    private JMSContext producerContext;

    @BeforeEach
    public void init() {
        factory = new ActiveMQJMSConnectionFactory("tcp://localhost:61616", null, null);
        producerContext = factory.createContext();
    }

    @AfterEach
    public void cleanup() {
        producerContext.close();
        factory.close();
    }

    private void sendMessages(String destination, int count) {
        Queue queue = producerContext.createQueue(destination);
        JMSProducer producer = producerContext.createProducer();
        for (int i = 0; i < count; i++) {
            producer.send(queue, i);
        }
    }

    @Test
    public void testLocalTransactionReceiveAndAck() {
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.jms.connector", JmsConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.jms.destination", "local-tx-ack")
                .with("mp.messaging.incoming.jms.transaction-mode", "local");
        addConfig(config);
        WeldContainer container = deploy(AckConsumerBean.class);

        AckConsumerBean bean = container.select(AckConsumerBean.class).get();

        sendMessages("local-tx-ack", 5);

        await().untilAsserted(() -> assertThat(bean.payloads()).hasSize(5));
        assertThat(bean.payloads()).containsExactly(0, 1, 2, 3, 4);
    }

    @Test
    public void testLocalTransactionSessionContextMetadata() {
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.jms.connector", JmsConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.jms.destination", "local-tx-metadata")
                .with("mp.messaging.incoming.jms.transaction-mode", "local");
        addConfig(config);
        WeldContainer container = deploy(MetadataConsumerBean.class);

        MetadataConsumerBean bean = container.select(MetadataConsumerBean.class).get();

        sendMessages("local-tx-metadata", 1);

        await().untilAsserted(() -> assertThat(bean.messages()).hasSize(1));

        IncomingJmsMessage<?> msg = bean.messages().get(0);
        JmsSessionContext sessionCtx = msg.getMetadata(JmsSessionContext.class).orElse(null);
        assertThat(sessionCtx).isNotNull();
        assertThat(sessionCtx.transactionMode()).isEqualTo(JmsTransactionMode.LOCAL);
        assertThat(sessionCtx.jmsContext()).isNotNull();
    }

    @Test
    public void testLocalTransactionWithSlowProcessing() {
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.jms.connector", JmsConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.jms.destination", "local-tx-slow")
                .with("mp.messaging.incoming.jms.transaction-mode", "local");
        addConfig(config);
        WeldContainer container = deploy(SlowConsumerBean.class);

        SlowConsumerBean bean = container.select(SlowConsumerBean.class).get();

        sendMessages("local-tx-slow", 5);

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(bean.payloads()).hasSize(5));
        assertThat(bean.payloads()).containsExactly(0, 1, 2, 3, 4);
    }

    @Test
    public void testLocalTransactionNackRollsBack() {
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.jms.connector", JmsConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.jms.destination", "local-tx-nack")
                .with("mp.messaging.incoming.jms.transaction-mode", "local")
                .with("mp.messaging.incoming.jms.failure-strategy", "ignore");
        addConfig(config);
        WeldContainer container = deploy(NackOnceConsumerBean.class);

        NackOnceConsumerBean bean = container.select(NackOnceConsumerBean.class).get();

        sendMessages("local-tx-nack", 3);

        await().untilAsserted(() -> {
            assertThat(bean.payloads().stream().filter(p -> p == 2).count())
                    .isGreaterThanOrEqualTo(1);
        });

        assertThat(bean.payloads().get(0)).isEqualTo(0);
        // Message 1 was nacked (rolled back), so it should be redelivered
        assertThat(bean.payloads().stream().filter(p -> p == 1).count()).isGreaterThanOrEqualTo(2);
    }

    @ApplicationScoped
    public static class AckConsumerBean {
        private final List<Integer> payloads = new CopyOnWriteArrayList<>();

        @Incoming("jms")
        public CompletionStage<Void> consume(IncomingJmsMessage<?> msg) {
            payloads.add((Integer) msg.getPayload());
            return msg.ack();
        }

        List<Integer> payloads() {
            return payloads;
        }
    }

    @ApplicationScoped
    public static class MetadataConsumerBean {
        private final List<IncomingJmsMessage<?>> messages = new CopyOnWriteArrayList<>();

        @Incoming("jms")
        public CompletionStage<Void> consume(IncomingJmsMessage<?> msg) {
            messages.add(msg);
            return msg.ack();
        }

        List<IncomingJmsMessage<?>> messages() {
            return messages;
        }
    }

    @ApplicationScoped
    public static class SlowConsumerBean {
        private final List<Integer> payloads = new CopyOnWriteArrayList<>();

        @Incoming("jms")
        public CompletionStage<Void> consume(IncomingJmsMessage<?> msg) throws InterruptedException {
            Thread.sleep(500);
            payloads.add((Integer) msg.getPayload());
            return msg.ack();
        }

        List<Integer> payloads() {
            return payloads;
        }
    }

    @ApplicationScoped
    public static class NackOnceConsumerBean {
        private final List<Integer> payloads = new CopyOnWriteArrayList<>();

        @Incoming("jms")
        public CompletionStage<Void> consume(IncomingJmsMessage<?> msg) {
            int payload = (Integer) msg.getPayload();
            payloads.add(payload);
            if (payload == 1 && payloads.stream().filter(p -> p == 1).count() == 1) {
                return msg.nack(new RuntimeException("simulated failure"));
            }
            return msg.ack();
        }

        List<Integer> payloads() {
            return payloads;
        }
    }
}
