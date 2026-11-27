package io.smallrye.reactive.messaging.jms;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.JMSProducer;
import jakarta.jms.Queue;

import org.apache.activemq.artemis.jms.client.ActiveMQJMSConnectionFactory;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.ChannelRegistry;
import io.smallrye.reactive.messaging.PausableChannel;
import io.smallrye.reactive.messaging.providers.extension.PausableChannelDecorator;
import io.smallrye.reactive.messaging.support.JmsTestBase;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

/**
 * With graceful shutdown, the in-flight tracking wraps each message, which loses the JMS message type. Methods
 * consuming {@link IncomingJmsMessage} get it restored ({@link JmsMessageTypeConverter}, registered by the test base),
 * and acknowledging the restored message is seen by the tracking.
 */
public class JmsMessageTypeRestoreTest extends JmsTestBase {

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

    private MapBasedConfig config(String destination) {
        return new MapBasedConfig()
                .with("mp.messaging.incoming.jms.connector", JmsConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.jms.destination", destination)
                .with("mp.messaging.incoming.jms.graceful-shutdown", true);
    }

    @Test
    void consumerReceivesIncomingJmsMessagesAndAcknowledgingCompletesTheDrain() throws Exception {
        addConfig(config("restore-type"));
        WeldContainer container = deploy(PausableChannelDecorator.class, HoldingConsumer.class);
        HoldingConsumer bean = container.select(HoldingConsumer.class).get();

        sendMessages("restore-type", 3);
        await().untilAsserted(() -> assertThat(bean.messages()).hasSize(3));

        List<IncomingJmsMessage<?>> messages = bean.messages();
        assertThat(messages).allSatisfy(message -> {
            // Tracked, then restored.
            assertThat(message).isInstanceOf(JmsMessageTypeConverter.RestoredIncomingJmsMessage.class);
            IncomingJmsMessageMetadata metadata = message.getMetadata(IncomingJmsMessageMetadata.class).orElseThrow();
            assertThat(messageId(message.unwrap(jakarta.jms.Message.class))).isEqualTo(metadata.getMessageId());
        });
        assertThat(messages).extracting(message -> (Integer) message.getPayload()).containsExactly(0, 1, 2);

        PausableChannel pausable = container.select(ChannelRegistry.class).get().getPausable("jms");
        assertThat(((PausableChannelDecorator.PauserChannel) pausable).getInFlightCount()).isEqualTo(3);

        CompletableFuture<Void> drain = pausable.pauseAndDrain().subscribeAsCompletionStage();
        // The messages are not acknowledged yet: the drain waits for them.
        await().pollDelay(Duration.ofMillis(300)).until(() -> true);
        assertThat(drain).isNotDone();

        for (IncomingJmsMessage<?> message : messages) {
            message.ack().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
        drain.get(10, TimeUnit.SECONDS);
        assertThat(((PausableChannelDecorator.PauserChannel) pausable).getInFlightCount()).isZero();
    }

    @Test
    void localTransactionsCommitAndRollBackThroughTheRestoredMessage() throws Exception {
        addConfig(config("restore-type-local-tx")
                .with("mp.messaging.incoming.jms.transaction-mode", "local")
                .with("mp.messaging.incoming.jms.failure-strategy", "ignore"));
        WeldContainer container = deploy(PausableChannelDecorator.class, NackOnceConsumer.class);
        NackOnceConsumer bean = container.select(NackOnceConsumer.class).get();

        sendMessages("restore-type-local-tx", 5);

        // Message 1 is nacked once (rolled back), then redelivered and committed: the transactions, and the sequential
        // receive they drive (the next message is received once the current one is committed or rolled back), work.
        await().untilAsserted(() -> assertThat(bean.payloads()).contains(0, 1, 2, 3, 4));
        assertThat(bean.messages()).allSatisfy(message -> {
            assertThat(message).isInstanceOf(JmsMessageTypeConverter.RestoredIncomingJmsMessage.class);
            assertThat(message.getMetadata(JmsSessionContext.class)).hasValueSatisfying(
                    context -> assertThat(context.transactionMode()).isEqualTo(JmsTransactionMode.LOCAL));
        });

        // Everything is committed or rolled back: nothing is in flight, the drain completes.
        PausableChannel pausable = container.select(ChannelRegistry.class).get().getPausable("jms");
        await().until(() -> ((PausableChannelDecorator.PauserChannel) pausable).getInFlightCount() == 0);
        pausable.pauseAndDrain().subscribeAsCompletionStage().get(10, TimeUnit.SECONDS);
    }

    private static String messageId(jakarta.jms.Message message) {
        try {
            return message.getJMSMessageID();
        } catch (JMSException e) {
            throw new IllegalStateException(e);
        }
    }

    @ApplicationScoped
    public static class HoldingConsumer {

        private final List<IncomingJmsMessage<?>> messages = new CopyOnWriteArrayList<>();

        @Incoming("jms")
        public CompletionStage<Void> consume(IncomingJmsMessage<?> message) {
            // Acknowledged later, by the test.
            messages.add(message);
            return CompletableFuture.completedFuture(null);
        }

        List<IncomingJmsMessage<?>> messages() {
            return messages;
        }
    }

    @ApplicationScoped
    public static class NackOnceConsumer {

        private final AtomicBoolean nacked = new AtomicBoolean();
        private final List<Integer> payloads = new CopyOnWriteArrayList<>();
        private final List<IncomingJmsMessage<?>> messages = new CopyOnWriteArrayList<>();

        @Incoming("jms")
        public CompletionStage<Void> consume(IncomingJmsMessage<?> message) {
            messages.add(message);
            int payload = (Integer) message.getPayload();
            if (payload == 1 && nacked.compareAndSet(false, true)) {
                return message.nack(new RuntimeException("simulated failure"));
            }
            payloads.add(payload);
            return message.ack();
        }

        List<Integer> payloads() {
            return payloads;
        }

        List<IncomingJmsMessage<?>> messages() {
            return messages;
        }
    }
}
