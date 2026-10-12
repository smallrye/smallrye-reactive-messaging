package io.smallrye.reactive.messaging.jms;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSProducer;
import jakarta.jms.Queue;
import jakarta.transaction.Status;

import org.apache.activemq.artemis.jms.client.ActiveMQXAConnectionFactory;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.support.JmsTestBase;
import io.smallrye.reactive.messaging.support.XaConnectionFactoryBean;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class JmsXaTransactionTest extends JmsTestBase {

    private ActiveMQXAConnectionFactory factory;
    private JMSContext producerContext;

    @Override
    protected boolean withConnectionFactory() {
        return false;
    }

    @Override
    protected Weld initWithoutConnectionFactory() {
        Weld weld = super.initWithoutConnectionFactory();
        weld.addBeanClass(XaConnectionFactoryBean.class);
        return weld;
    }

    @BeforeEach
    public void init() {
        factory = new ActiveMQXAConnectionFactory("tcp://localhost:61616", null, null);
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

    // --- Basic XA receive + ack (commit) ---

    @Test
    public void testXaTransactionReceiveAndAck() {
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.jms.connector", JmsConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.jms.destination", "xa-tx-ack")
                .with("mp.messaging.incoming.jms.transaction-mode", "xa");
        addConfig(config);
        WeldContainer container = deploy(AckConsumerBean.class);

        AckConsumerBean bean = container.select(AckConsumerBean.class).get();

        sendMessages("xa-tx-ack", 5);

        await().untilAsserted(() -> assertThat(bean.payloads()).hasSize(5));
        assertThat(bean.payloads()).containsExactly(0, 1, 2, 3, 4);
    }

    // --- XA receive + nack (rollback + redelivery) ---

    @Test
    public void testXaTransactionNackRollsBack() {
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.jms.connector", JmsConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.jms.destination", "xa-tx-nack")
                .with("mp.messaging.incoming.jms.transaction-mode", "xa")
                .with("mp.messaging.incoming.jms.failure-strategy", "ignore");
        addConfig(config);
        WeldContainer container = deploy(NackOnceConsumerBean.class);

        NackOnceConsumerBean bean = container.select(NackOnceConsumerBean.class).get();

        sendMessages("xa-tx-nack", 3);

        // Message 1 is nacked once (rolled back) then redelivered and accepted.
        // All 3 messages must eventually be consumed — the stream continues after the rollback.
        await().untilAsserted(() -> assertThat(bean.payloads()).contains(0, 1, 2));
    }

    // --- XA message carries JmsXaTransactionMetadata and JmsSessionContext ---

    @Test
    public void testXaTransactionMetadata() {
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.jms.connector", JmsConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.jms.destination", "xa-tx-metadata")
                .with("mp.messaging.incoming.jms.transaction-mode", "xa");
        addConfig(config);
        WeldContainer container = deploy(MetadataConsumerBean.class);

        MetadataConsumerBean bean = container.select(MetadataConsumerBean.class).get();

        sendMessages("xa-tx-metadata", 1);

        await().untilAsserted(() -> assertThat(bean.received()).isTrue());

        assertThat(bean.sessionContext()).isNotNull();
        assertThat(bean.sessionContext().transactionMode()).isEqualTo(JmsTransactionMode.XA);
        assertThat(bean.sessionContext().jmsContext()).isNotNull();

        assertThat(bean.xaMetadata()).isNotNull();
        assertThat(bean.xaMetadata().transaction()).isNotNull();
    }

    // --- Transaction usability: withTransaction scope API ---

    @Test
    public void testXaTransactionWithTransactionScope() {
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.jms.connector", JmsConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.jms.destination", "xa-tx-resume")
                .with("mp.messaging.incoming.jms.transaction-mode", "xa");
        addConfig(config);
        WeldContainer container = deploy(WithTransactionScopeBean.class);

        WithTransactionScopeBean bean = container.select(WithTransactionScopeBean.class).get();

        sendMessages("xa-tx-resume", 3);

        await().untilAsserted(() -> assertThat(bean.payloads()).hasSize(3));
        assertThat(bean.payloads()).containsExactly(0, 1, 2);
        assertThat(bean.transactionWasActive()).isTrue();
    }

    // --- Incoming → outgoing processor with XA session sharing via connector ---

    @Test
    public void testXaTransactionIncomingOutgoingProcessor() {
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.source.connector", JmsConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.source.destination", "xa-proc-in")
                .with("mp.messaging.incoming.source.transaction-mode", "xa")
                .with("mp.messaging.outgoing.sink.connector", JmsConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.sink.destination", "xa-proc-out");
        addConfig(config);
        WeldContainer container = deploy(ProcessorBean.class);

        sendMessages("xa-proc-in", 3);

        try (var consumer = producerContext.createConsumer(producerContext.createQueue("xa-proc-out"))) {
            List<String> received = new CopyOnWriteArrayList<>();
            await().untilAsserted(() -> {
                jakarta.jms.Message m;
                while ((m = consumer.receive(100)) != null) {
                    received.add(m.getBody(String.class));
                }
                assertThat(received).hasSize(3);
            });
            assertThat(received).containsExactlyInAnyOrder("processed-0", "processed-1", "processed-2");
        }
    }

    // --- Incoming → outgoing: rollback-only undoes outbound send ---

    @Test
    public void testXaTransactionProcessorRollbackUndoesSend() {
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.source.connector", JmsConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.source.destination", "xa-proc-rollback-in")
                .with("mp.messaging.incoming.source.transaction-mode", "xa")
                .with("mp.messaging.incoming.source.failure-strategy", "ignore")
                .with("mp.messaging.outgoing.sink.connector", JmsConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.sink.destination", "xa-proc-rollback-out");
        addConfig(config);
        WeldContainer container = deploy(RollbackProcessorBean.class);

        RollbackProcessorBean bean = container.select(RollbackProcessorBean.class).get();

        sendMessages("xa-proc-rollback-in", 1);

        await().untilAsserted(() -> assertThat(bean.received()).isTrue());

        // The processor returned a value (so the send happened via JmsSink),
        // but marked the transaction rollback-only, so the commit rolled back the send
        try (var consumer = producerContext.createConsumer(producerContext.createQueue("xa-proc-rollback-out"))) {
            jakarta.jms.Message stray = consumer.receive(2000);
            assertThat(stray).as("Outbound message should have been rolled back").isNull();
        }
    }

    // --- Nack with default fail strategy ---

    @Test
    public void testXaTransactionNackWithFailStrategy() {
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.jms.connector", JmsConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.jms.destination", "xa-tx-nack-fail")
                .with("mp.messaging.incoming.jms.transaction-mode", "xa");
        addConfig(config);
        WeldContainer container = deploy(FailingConsumerBean.class);

        FailingConsumerBean bean = container.select(FailingConsumerBean.class).get();

        sendMessages("xa-tx-nack-fail", 1);

        await().untilAsserted(() -> assertThat(bean.received()).isTrue());
    }

    // --- Transaction rolled back before ack (simulates timeout or external rollback) ---

    @Test
    public void testXaTransactionRolledBackBeforeAck() {
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.jms.connector", JmsConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.jms.destination", "xa-tx-rollback-before-ack")
                .with("mp.messaging.incoming.jms.transaction-mode", "xa")
                .with("mp.messaging.incoming.jms.failure-strategy", "ignore");
        addConfig(config);
        WeldContainer container = deploy(RollbackBeforeAckBean.class);

        RollbackBeforeAckBean bean = container.select(RollbackBeforeAckBean.class).get();

        sendMessages("xa-tx-rollback-before-ack", 3);

        // Message 0 has its transaction aborted, but the stream continues and all 3 are consumed
        await().untilAsserted(() -> assertThat(bean.payloads()).contains(0, 1, 2));
        assertThat(bean.wasRolledBack()).isTrue();
    }

    // --- Bean definitions ---

    @ApplicationScoped
    public static class AckConsumerBean {
        private final List<Integer> payloads = new CopyOnWriteArrayList<>();

        @Incoming("jms")
        public void consume(int payload) {
            payloads.add(payload);
        }

        List<Integer> payloads() {
            return payloads;
        }
    }

    @ApplicationScoped
    public static class MetadataConsumerBean {
        private volatile JmsSessionContext sessionContext;
        private volatile JmsXaTransactionMetadata xaMetadata;
        private final AtomicBoolean received = new AtomicBoolean();

        @Incoming("jms")
        public void consume(int payload, JmsSessionContext sessionCtx, JmsXaTransactionMetadata xaMeta) {
            this.sessionContext = sessionCtx;
            this.xaMetadata = xaMeta;
            received.set(true);
        }

        boolean received() {
            return received.get();
        }

        JmsSessionContext sessionContext() {
            return sessionContext;
        }

        JmsXaTransactionMetadata xaMetadata() {
            return xaMetadata;
        }
    }

    @ApplicationScoped
    public static class NackOnceConsumerBean {
        private final AtomicBoolean nacked = new AtomicBoolean();
        private final List<Integer> payloads = new CopyOnWriteArrayList<>();

        @Incoming("jms")
        public void consume(int payload) {
            if (payload == 1 && nacked.compareAndSet(false, true)) {
                throw new RuntimeException("simulated failure");
            }
            payloads.add(payload);
        }

        List<Integer> payloads() {
            return payloads;
        }
    }

    @ApplicationScoped
    public static class WithTransactionScopeBean {
        private final List<Integer> payloads = new CopyOnWriteArrayList<>();
        private final AtomicBoolean transactionActive = new AtomicBoolean();

        @Incoming("jms")
        public void consume(int payload, JmsXaTransactionMetadata xa) {
            try (var scope = xa.withTransaction()) {
                if (xa.status() == Status.STATUS_ACTIVE) {
                    transactionActive.set(true);
                }
                payloads.add(payload);
            }
        }

        List<Integer> payloads() {
            return payloads;
        }

        boolean transactionWasActive() {
            return transactionActive.get();
        }
    }

    @ApplicationScoped
    public static class ProcessorBean {

        @Incoming("source")
        @Outgoing("sink")
        public String process(int payload) {
            return "processed-" + payload;
        }
    }

    @ApplicationScoped
    public static class RollbackProcessorBean {
        private final AtomicBoolean received = new AtomicBoolean();

        @Incoming("source")
        @Outgoing("sink")
        public String process(int payload, JmsXaTransactionMetadata xa) throws Exception {
            received.set(true);
            xa.transaction().setRollbackOnly();
            return "should-not-arrive-" + payload;
        }

        boolean received() {
            return received.get();
        }
    }

    @ApplicationScoped
    public static class FailingConsumerBean {
        private final AtomicBoolean received = new AtomicBoolean();

        @Incoming("jms")
        public void consume(int payload) {
            received.set(true);
            throw new RuntimeException("intentional failure");
        }

        boolean received() {
            return received.get();
        }
    }

    @ApplicationScoped
    public static class RollbackBeforeAckBean {
        private final AtomicBoolean wasRolledBack = new AtomicBoolean();
        private final List<Integer> payloads = new CopyOnWriteArrayList<>();

        @Incoming("jms")
        public void consume(int payload, JmsXaTransactionMetadata xa) {
            if (payload == 0 && wasRolledBack.compareAndSet(false, true)) {
                xa.rollback();
                return;
            }
            payloads.add(payload);
        }

        List<Integer> payloads() {
            return payloads;
        }

        boolean wasRolledBack() {
            return wasRolledBack.get();
        }
    }
}
