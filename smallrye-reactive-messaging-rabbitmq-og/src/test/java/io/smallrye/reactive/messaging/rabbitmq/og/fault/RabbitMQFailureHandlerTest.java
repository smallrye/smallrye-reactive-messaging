package io.smallrye.reactive.messaging.rabbitmq.og.fault;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.rabbitmq.og.IncomingRabbitMQMetadata;
import io.smallrye.reactive.messaging.rabbitmq.og.RabbitMQConnector;
import io.smallrye.reactive.messaging.rabbitmq.og.RabbitMQRejectMetadata;
import io.smallrye.reactive.messaging.rabbitmq.og.WeldTestBase;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class RabbitMQFailureHandlerTest extends WeldTestBase {

    @Override
    @BeforeEach
    public void initWeld() {
        super.initWeld();
        weld.addBeanClass(RabbitMQRequeue.Factory.class);
    }

    private MapBasedConfig dataconfig(String failureStrategy) {
        return commonConfig()
                .with("mp.messaging.incoming.data.connector", RabbitMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.queue.name", queueName)
                .with("mp.messaging.incoming.data.exchange.name", exchangeName)
                .with("mp.messaging.incoming.data.exchange.routing-keys", routingKeys)
                .with("mp.messaging.incoming.data.tracing.enabled", false)
                .with("mp.messaging.incoming.data.failure-strategy", failureStrategy);
    }

    private void produceMessages() {
        AtomicInteger counter = new AtomicInteger(1);
        usage.produce(exchangeName, queueName, routingKeys, 5, counter::getAndIncrement);
    }

    @Test
    void rejectStrategyDropsMessages() {
        RejectBean bean = runApplication(dataconfig("reject"), RejectBean.class);
        produceMessages();

        await().until(() -> bean.getProcessed().size() >= 5);
        assertThat(bean.getProcessed()).containsExactly(1, 2, 3, 4, 5);
        // With reject (requeue=false by default), messages are dropped, not redelivered
        assertThat(bean.getRedelivered()).isEmpty();
    }

    @Test
    void requeueStrategyRequeuesMessages() {
        RequeueBean bean = runApplication(dataconfig("requeue"), RequeueBean.class);
        produceMessages();

        // Each message is nacked on first delivery (requeued), then acked on redelivery
        await().until(() -> bean.getRedelivered().size() >= 5);
        assertThat(bean.getFirstDeliveries()).containsExactlyInAnyOrder(1, 2, 3, 4, 5);
        assertThat(bean.getRedelivered()).containsExactlyInAnyOrder(1, 2, 3, 4, 5);
    }

    @Test
    void rejectWithRequeueMetadataOverride() {
        RejectWithRequeueOverrideBean bean = runApplication(dataconfig("reject"),
                RejectWithRequeueOverrideBean.class);
        produceMessages();

        // Despite reject strategy (default requeue=false), metadata overrides to requeue=true
        await().until(() -> bean.getRedelivered().size() >= 5);
        assertThat(bean.getFirstDeliveries()).containsExactlyInAnyOrder(1, 2, 3, 4, 5);
        assertThat(bean.getRedelivered()).containsExactlyInAnyOrder(1, 2, 3, 4, 5);
    }

    @Test
    void requeueWithNoRequeueMetadataOverride() {
        RequeueWithNoRequeueOverrideBean bean = runApplication(dataconfig("requeue"),
                RequeueWithNoRequeueOverrideBean.class);
        produceMessages();

        await().until(() -> bean.getProcessed().size() >= 5);
        assertThat(bean.getProcessed()).containsExactly(1, 2, 3, 4, 5);
        // Despite requeue strategy (default requeue=true), metadata overrides to requeue=false
        assertThat(bean.getRedelivered()).isEmpty();
    }

    // --- Inner beans ---

    /**
     * Nacks every message (reject strategy will drop them).
     */
    @ApplicationScoped
    public static class RejectBean {
        private final List<Integer> processed = new CopyOnWriteArrayList<>();
        private final List<Integer> redelivered = new CopyOnWriteArrayList<>();

        @Incoming("data")
        public CompletionStage<Void> process(Message<String> msg) {
            int value = Integer.parseInt(msg.getPayload());
            processed.add(value);
            boolean redeliver = msg.getMetadata(IncomingRabbitMQMetadata.class)
                    .map(IncomingRabbitMQMetadata::isRedeliver)
                    .orElse(false);
            if (redeliver) {
                redelivered.add(value);
            }
            return msg.nack(new RuntimeException("reject"));
        }

        public List<Integer> getProcessed() {
            return processed;
        }

        public List<Integer> getRedelivered() {
            return redelivered;
        }
    }

    /**
     * Nacks on first delivery, acks on redelivery (requeue strategy will requeue).
     */
    @ApplicationScoped
    public static class RequeueBean {
        private final List<Integer> firstDeliveries = new CopyOnWriteArrayList<>();
        private final List<Integer> redelivered = new CopyOnWriteArrayList<>();

        @Incoming("data")
        public CompletionStage<Void> process(Message<String> msg) {
            int value = Integer.parseInt(msg.getPayload());
            boolean redeliver = msg.getMetadata(IncomingRabbitMQMetadata.class)
                    .map(IncomingRabbitMQMetadata::isRedeliver)
                    .orElse(false);
            if (redeliver) {
                redelivered.add(value);
                return msg.ack();
            } else {
                firstDeliveries.add(value);
                return msg.nack(new RuntimeException("requeue"));
            }
        }

        public List<Integer> getFirstDeliveries() {
            return firstDeliveries;
        }

        public List<Integer> getRedelivered() {
            return redelivered;
        }
    }

    /**
     * Uses reject strategy but overrides requeue to true via metadata.
     * Nacks on first delivery with requeue=true metadata, acks on redelivery.
     */
    @ApplicationScoped
    public static class RejectWithRequeueOverrideBean {
        private final List<Integer> firstDeliveries = new CopyOnWriteArrayList<>();
        private final List<Integer> redelivered = new CopyOnWriteArrayList<>();

        @Incoming("data")
        public CompletionStage<Void> process(Message<String> msg) {
            int value = Integer.parseInt(msg.getPayload());
            boolean redeliver = msg.getMetadata(IncomingRabbitMQMetadata.class)
                    .map(IncomingRabbitMQMetadata::isRedeliver)
                    .orElse(false);
            if (redeliver) {
                redelivered.add(value);
                return msg.ack();
            } else {
                firstDeliveries.add(value);
                return msg.nack(new RuntimeException("reject-with-requeue"),
                        Metadata.of(new RabbitMQRejectMetadata(true)));
            }
        }

        public List<Integer> getFirstDeliveries() {
            return firstDeliveries;
        }

        public List<Integer> getRedelivered() {
            return redelivered;
        }
    }

    /**
     * Uses requeue strategy but overrides requeue to false via metadata.
     * Nacks every message with requeue=false metadata.
     */
    @ApplicationScoped
    public static class RequeueWithNoRequeueOverrideBean {
        private final List<Integer> processed = new CopyOnWriteArrayList<>();
        private final List<Integer> redelivered = new CopyOnWriteArrayList<>();

        @Incoming("data")
        public CompletionStage<Void> process(Message<String> msg) {
            int value = Integer.parseInt(msg.getPayload());
            processed.add(value);
            boolean redeliver = msg.getMetadata(IncomingRabbitMQMetadata.class)
                    .map(IncomingRabbitMQMetadata::isRedeliver)
                    .orElse(false);
            if (redeliver) {
                redelivered.add(value);
            }
            return msg.nack(new RuntimeException("no-requeue"),
                    Metadata.of(new RabbitMQRejectMetadata(false)));
        }

        public List<Integer> getProcessed() {
            return processed;
        }

        public List<Integer> getRedelivered() {
            return redelivered;
        }
    }
}
