package io.smallrye.reactive.messaging.jms.fault;

import static io.smallrye.reactive.messaging.jms.fault.JmsDlqFailure.DEAD_LETTER_CAUSE;
import static io.smallrye.reactive.messaging.jms.fault.JmsDlqFailure.DEAD_LETTER_CAUSE_CLASS_NAME;
import static io.smallrye.reactive.messaging.jms.fault.JmsDlqFailure.DEAD_LETTER_EXCEPTION_CLASS_NAME;
import static io.smallrye.reactive.messaging.jms.fault.JmsDlqFailure.DEAD_LETTER_REASON;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.lang.reflect.Method;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSConsumer;
import jakarta.jms.JMSContext;
import jakarta.jms.Queue;
import jakarta.jms.TextMessage;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.jms.JmsConnector;
import io.smallrye.reactive.messaging.providers.extension.HealthCenter;
import io.smallrye.reactive.messaging.support.JmsTestBase;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class JmsFailureHandlerTest extends JmsTestBase {

    private static final int FAILURE_TRIGGER_MODULO = 3;
    private static final int MESSAGE_COUNT = 10;
    private static final int EXPECTED_FAILURES = 3; // Messages 3, 6, 9

    private WeldContainer container;
    private String destination;

    @BeforeEach
    public void initTopic(TestInfo testInfo) {
        String cn = testInfo.getTestClass().map(Class::getSimpleName).orElse(UUID.randomUUID().toString());
        String mn = testInfo.getTestMethod().map(Method::getName).orElse(UUID.randomUUID().toString());
        destination = cn + "-" + mn + "-" + UUID.randomUUID().getMostSignificantBits();
    }

    @Test
    public void testFailStrategy() {
        MyReceiverBean bean = runApplication(getFailConfig(destination), MyReceiverBean.class);

        produceIntegers();

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.list().size() >= 4);
        // Other records should not have been received.
        assertThat(bean.list()).containsExactly(0, 1, 2, 3);

        // Verify that the stream failed
        await().atMost(10, TimeUnit.SECONDS).until(() -> !isAlive());
    }

    @Test
    public void testFailStrategyWithPayload() {
        MyReceiverBeanUsingPayload bean = runApplication(getFailConfig(destination), MyReceiverBeanUsingPayload.class);

        produceIntegers();

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.list().size() >= 4);
        // Other records should not have been received.
        assertThat(bean.list()).containsExactly(0, 1, 2, 3);

        // Verify that the stream failed
        await().atMost(10, TimeUnit.SECONDS).until(() -> !isAlive());
    }

    @Test
    public void testIgnoreStrategy() {
        MyReceiverBean bean = runApplication(getIgnoreConfig(destination), MyReceiverBean.class);

        produceIntegers();

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.list().size() >= 10);
        // All records should have been received.
        assertThat(bean.list()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        assertThat(isAlive()).isTrue();
    }

    @Test
    public void testIgnoreStrategyWithPayload() {
        MyReceiverBeanUsingPayload bean = runApplication(getIgnoreConfig(destination),
                MyReceiverBeanUsingPayload.class);

        produceIntegers();

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.list().size() >= 10);
        // All records should have been received.
        assertThat(bean.list()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        assertThat(isAlive()).isTrue();
    }

    @Test
    public void testDeadLetterQueueStrategyWithDefaultDestination() {
        String dlqDestination = "dead-letter-queue-jms";

        MyReceiverBean bean = runApplication(getDeadLetterQueueConfig(destination), MyReceiverBean.class);

        produceIntegers();

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.list().size() >= 10);
        assertThat(bean.list()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        // Verify DLQ messages
        ConnectionFactory cf = container.getBeanManager().createInstance().select(ConnectionFactory.class).get();
        List<jakarta.jms.Message> dlqMessages = consumeMessages(cf, dlqDestination, EXPECTED_FAILURES);

        assertThat(dlqMessages).hasSize(EXPECTED_FAILURES).allSatisfy(msg -> {
            try {
                assertThat(msg).isInstanceOf(TextMessage.class);
                int value = Integer.parseInt(((TextMessage) msg).getText());
                assertThat(value).isIn(3, 6, 9);

                // Verify DLQ metadata headers
                assertThat(msg.getStringProperty(DEAD_LETTER_EXCEPTION_CLASS_NAME))
                        .isEqualTo(IllegalArgumentException.class.getName());
                assertThat(msg.getStringProperty(DEAD_LETTER_REASON)).startsWith("nack 3 -");
                assertThat(msg.getObjectProperty(DEAD_LETTER_CAUSE)).isNull();
                assertThat(msg.getObjectProperty(DEAD_LETTER_CAUSE_CLASS_NAME)).isNull();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        assertThat(isAlive()).isTrue();
    }

    @Test
    public void testDeadLetterQueueStrategyWithCustomDestination() {
        String customDlq = destination + "-custom-dlq";

        MyReceiverBean bean = runApplication(getDeadLetterQueueWithCustomConfig(destination, customDlq),
                MyReceiverBean.class);

        produceIntegers();

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.list().size() >= 10);
        assertThat(bean.list()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        // Verify DLQ messages in custom destination
        ConnectionFactory cf = container.getBeanManager().createInstance().select(ConnectionFactory.class).get();
        List<jakarta.jms.Message> dlqMessages = consumeMessages(cf, customDlq, EXPECTED_FAILURES);

        assertThat(dlqMessages).hasSize(EXPECTED_FAILURES).allSatisfy(msg -> {
            try {
                assertThat(msg).isInstanceOf(TextMessage.class);
                int value = Integer.parseInt(((TextMessage) msg).getText());
                assertThat(value).isIn(3, 6, 9);
                // Verify DLQ metadata
                assertThat(msg.getStringProperty(DEAD_LETTER_EXCEPTION_CLASS_NAME))
                        .isEqualTo(IllegalArgumentException.class.getName());
                assertThat(msg.getStringProperty(DEAD_LETTER_REASON)).startsWith("nack 3 -");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        assertThat(isAlive()).isTrue();
    }

    @Test
    public void testDeadLetterQueueStrategyWithPayload() {
        String customDlq = destination + "-dlq";

        MyReceiverBeanUsingPayload bean = runApplication(getDeadLetterQueueWithCustomConfig(destination, customDlq),
                MyReceiverBeanUsingPayload.class);

        produceIntegers();

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.list().size() >= 10);
        assertThat(bean.list()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        // Verify DLQ messages
        ConnectionFactory cf = container.getBeanManager().createInstance().select(ConnectionFactory.class).get();
        List<jakarta.jms.Message> dlqMessages = consumeMessages(cf, customDlq, EXPECTED_FAILURES);

        assertThat(dlqMessages).hasSize(EXPECTED_FAILURES).allSatisfy(msg -> {
            try {
                assertThat(msg).isInstanceOf(TextMessage.class);
                int value = Integer.parseInt(((TextMessage) msg).getText());
                assertThat(value).isIn(3, 6, 9);
                // Verify DLQ metadata
                assertThat(msg.getStringProperty(DEAD_LETTER_EXCEPTION_CLASS_NAME))
                        .isEqualTo(IllegalArgumentException.class.getName());
                assertThat(msg.getStringProperty(DEAD_LETTER_REASON)).startsWith("nack 3 -");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        assertThat(isAlive()).isTrue();
    }

    @Test
    public void testDeadLetterQueueWithCausedException() {
        String customDlq = destination + "-dlq";

        MyReceiverBeanWithCause bean = runApplication(getDeadLetterQueueWithCustomConfig(destination, customDlq),
                MyReceiverBeanWithCause.class);

        produceIntegers();

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.list().size() >= 10);
        assertThat(bean.list()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        // Verify DLQ messages with cause information
        ConnectionFactory cf = container.getBeanManager().createInstance().select(ConnectionFactory.class).get();
        List<jakarta.jms.Message> dlqMessages = consumeMessages(cf, customDlq, EXPECTED_FAILURES);

        assertThat(dlqMessages).hasSize(EXPECTED_FAILURES).allSatisfy(msg -> {
            try {
                assertThat(msg).isInstanceOf(TextMessage.class);
                int value = Integer.parseInt(((TextMessage) msg).getText());
                assertThat(value).isIn(3, 6, 9);

                // Verify exception and cause metadata
                assertThat(msg.getStringProperty(DEAD_LETTER_EXCEPTION_CLASS_NAME))
                        .isEqualTo(IllegalStateException.class.getName());
                assertThat(msg.getStringProperty(DEAD_LETTER_REASON)).contains("Processing failed");
                assertThat(msg.getStringProperty(DEAD_LETTER_CAUSE_CLASS_NAME))
                        .isEqualTo(NullPointerException.class.getName());
                assertThat(msg.getStringProperty(DEAD_LETTER_CAUSE)).isEqualTo("Root cause");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        assertThat(isAlive()).isTrue();
    }

    private MapBasedConfig getFailConfig(String destination) {
        return new MapBasedConfig()
                .with("mp.messaging.incoming.jms.connector", JmsConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.jms.destination", destination)
                .with("mp.messaging.incoming.jms.durable", false)
                .with("mp.messaging.incoming.jms.tracing.enabled", false)
                .with("mp.messaging.incoming.jms.failure-strategy", JmsFailureHandler.Strategy.FAIL);
    }

    private MapBasedConfig getIgnoreConfig(String destination) {
        return new MapBasedConfig()
                .with("mp.messaging.incoming.jms.connector", JmsConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.jms.destination", destination)
                .with("mp.messaging.incoming.jms.durable", false)
                .with("mp.messaging.incoming.jms.tracing.enabled", false)
                .with("mp.messaging.incoming.jms.failure-strategy", JmsFailureHandler.Strategy.IGNORE);
    }

    private MapBasedConfig getDeadLetterQueueConfig(String destination) {
        return new MapBasedConfig()
                .with("mp.messaging.incoming.jms.connector", JmsConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.jms.destination", destination)
                .with("mp.messaging.incoming.jms.durable", false)
                .with("mp.messaging.incoming.jms.tracing.enabled", false)
                .with("mp.messaging.incoming.jms.failure-strategy", JmsFailureHandler.Strategy.DEAD_LETTER_QUEUE);
    }

    private MapBasedConfig getDeadLetterQueueWithCustomConfig(String destination, String dlqDestination) {
        return new MapBasedConfig()
                .with("mp.messaging.incoming.jms.connector", JmsConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.jms.destination", destination)
                .with("mp.messaging.incoming.jms.durable", false)
                .with("mp.messaging.incoming.jms.tracing.enabled", false)
                .with("mp.messaging.incoming.jms.failure-strategy", JmsFailureHandler.Strategy.DEAD_LETTER_QUEUE)
                .with("mp.messaging.incoming.jms.dead-letter-queue.destination", dlqDestination);
    }

    private void produceIntegers() {
        ConnectionFactory cf = container.getBeanManager().createInstance().select(ConnectionFactory.class).get();
        AtomicInteger counter = new AtomicInteger(0);
        produceIntegers(cf, destination, MESSAGE_COUNT, counter::getAndIncrement);
    }

    private <T> T runApplication(MapBasedConfig config, Class<T> beanClass) {
        config.write();
        container = deploy(beanClass);
        return container.getBeanManager().createInstance().select(beanClass).get();
    }

    private boolean isAlive() {
        HealthCenter health = container.getBeanManager().createInstance().select(HealthCenter.class).get();
        HealthReport liveness = health.getLiveness();
        return liveness.isOk();
    }

    private List<jakarta.jms.Message> consumeMessages(ConnectionFactory cf, String destination, int expectedCount) {
        List<jakarta.jms.Message> messages = new CopyOnWriteArrayList<>();
        try (JMSContext context = cf.createContext()) {
            Queue queue = context.createQueue(destination);
            JMSConsumer consumer = context.createConsumer(queue);

            await().atMost(10, TimeUnit.SECONDS).until(() -> {
                jakarta.jms.Message msg = consumer.receiveNoWait();
                if (msg != null) {
                    messages.add(msg);
                }
                return messages.size() >= expectedCount;
            });
        }
        return messages;
    }

    @ApplicationScoped
    public static class MyReceiverBean {
        private final List<Integer> received = new CopyOnWriteArrayList<>();

        @Incoming("jms")
        public CompletionStage<Void> process(Message<Integer> message) {
            Integer payload = message.getPayload();
            received.add(payload);
            if (payload != 0 && payload % FAILURE_TRIGGER_MODULO == 0) {
                return message.nack(new IllegalArgumentException("nack 3 - " + payload));
            }
            return message.ack();
        }

        public List<Integer> list() {
            return received;
        }
    }

    @ApplicationScoped
    public static class MyReceiverBeanUsingPayload {
        private final List<Integer> received = new CopyOnWriteArrayList<>();

        @Incoming("jms")
        public Uni<Void> process(int value) {
            received.add(value);
            if (value != 0 && value % FAILURE_TRIGGER_MODULO == 0) {
                return Uni.createFrom().failure(new IllegalArgumentException("nack 3 - " + value));
            }
            return Uni.createFrom().nullItem();
        }

        public List<Integer> list() {
            return received;
        }
    }

    @ApplicationScoped
    public static class MyReceiverBeanWithCause {
        private final List<Integer> received = new CopyOnWriteArrayList<>();

        @Incoming("jms")
        public CompletionStage<Void> process(Message<Integer> message) {
            Integer payload = message.getPayload();
            received.add(payload);
            if (payload != 0 && payload % FAILURE_TRIGGER_MODULO == 0) {
                NullPointerException cause = new NullPointerException("Root cause");
                IllegalStateException exception = new IllegalStateException("Processing failed for " + payload, cause);
                return message.nack(exception);
            }
            return message.ack();
        }

        public List<Integer> list() {
            return received;
        }
    }
}
