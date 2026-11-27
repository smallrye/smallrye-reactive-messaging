package io.smallrye.reactive.messaging.pulsar;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.pulsar.base.WeldTestBase;
import io.smallrye.reactive.messaging.pulsar.transactions.PulsarTransactions;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

/**
 * With graceful shutdown enabled, the in-flight tracking rebuilds the incoming messages, which lose the Pulsar message
 * type. Methods consuming the Pulsar message types get them restored, and acknowledging them, also inside a
 * transaction, acknowledges the Pulsar messages and completes the tracking.
 */
public class PulsarMessageTypeRestoreTest extends WeldTestBase {

    private static final int NUMBER_OF_MESSAGES = 10;
    private static final String SUBSCRIPTION = "restore-subscription";

    @Test
    void messageConsumerReceivesRestoredPulsarMessages() throws PulsarClientException {
        MessageConsumer bean = runApplication(config(topic), MessageConsumer.class);

        send(client.newProducer(Schema.INT32)
                .producerName(topic + "-producer")
                .topic(topic)
                .create(), NUMBER_OF_MESSAGES, (i, p) -> p.newMessage().key("k-" + i).value(i));

        await().atMost(Duration.ofSeconds(30)).until(() -> bean.received().size() == NUMBER_OF_MESSAGES);
        assertThat(bean.received())
                .allSatisfy(m -> assertThat(m).isInstanceOf(PulsarMessageTypeConverter.RestoredPulsarMessage.class))
                .extracting(PulsarIncomingMessage::getKey).contains("k-0", "k-9");
        assertThat(bean.received()).extracting(PulsarIncomingMessage::getMessageId).doesNotContainNull();

        assertAcknowledged(topic);
        // Every message is acknowledged: the graceful shutdown does not wait for the drain timeout.
        container.close();
        container = null;
    }

    @Test
    void batchConsumerReceivesRestoredPulsarBatchMessages() throws PulsarClientException {
        BatchConsumer bean = runApplication(config(topic)
                .with("mp.messaging.incoming.data.batchReceive", true), BatchConsumer.class);

        send(client.newProducer(Schema.INT32)
                .producerName(topic + "-producer")
                .topic(topic)
                .create(), NUMBER_OF_MESSAGES, (i, p) -> p.newMessage().key("k-" + i).value(i));

        await().atMost(Duration.ofSeconds(30)).until(() -> bean.received().size() == NUMBER_OF_MESSAGES);
        assertThat(bean.batches())
                .allSatisfy(b -> assertThat(b).isInstanceOf(PulsarMessageTypeConverter.RestoredPulsarBatchMessage.class));

        assertAcknowledged(topic);
        container.close();
        container = null;
    }

    @Test
    void exactlyOnceProcessorAcknowledgesRestoredMessagesInTheTransaction()
            throws PulsarAdminException, PulsarClientException {
        String inTopic = UUID.randomUUID().toString();
        createPartitionedTopic(inTopic, 3);
        String outTopic = UUID.randomUUID().toString();
        createPartitionedTopic(outTopic, 3);
        MapBasedConfig config = config(inTopic)
                .with("mp.messaging.incoming.data.enableTransaction", true)
                .with("mp.messaging.outgoing.transactional-producer.connector", PulsarConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.transactional-producer.serviceUrl", serviceUrl)
                .with("mp.messaging.outgoing.transactional-producer.topic", outTopic)
                .with("mp.messaging.outgoing.transactional-producer.enableTransaction", true)
                .with("mp.messaging.outgoing.transactional-producer.schema", "INT32");
        ExactlyOnceProcessor bean = runApplication(config, ExactlyOnceProcessor.class);

        send(client.newProducer(Schema.INT32)
                .producerName("test-producer")
                .topic(inTopic)
                .create(), NUMBER_OF_MESSAGES, (i, p) -> p.newMessage().key("k-" + i).value(i));

        List<Integer> list = new CopyOnWriteArrayList<>();
        receive(client.newConsumer(Schema.INT32)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .consumerName("test-consumer")
                .subscriptionName("test-subscription")
                .topic(outTopic)
                .subscribe(), NUMBER_OF_MESSAGES, m -> list.add(m.getValue()));

        await().atMost(Duration.ofMinutes(1)).untilAsserted(() -> assertThat(list)
                .containsAll(IntStream.range(0, NUMBER_OF_MESSAGES).boxed().collect(Collectors.toList()))
                .doesNotHaveDuplicates());
        assertThat(bean.received())
                .allSatisfy(m -> assertThat(m).isInstanceOf(PulsarMessageTypeConverter.RestoredPulsarMessage.class));
        // The messages are acknowledged in the committed transactions.
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(admin.topics()
                .getPartitionedStats(inTopic, false).getSubscriptions().get(SUBSCRIPTION).getMsgBacklog()).isZero());
        container.close();
        container = null;
    }

    private void assertAcknowledged(String topic) {
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(admin.topics()
                .getStats(topic).getSubscriptions().get(SUBSCRIPTION).getMsgBacklog()).isZero());
    }

    private MapBasedConfig config(String topic) {
        return new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", PulsarConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.serviceUrl", serviceUrl)
                .with("mp.messaging.incoming.data.topic", topic)
                .with("mp.messaging.incoming.data.subscriptionName", SUBSCRIPTION)
                .with("mp.messaging.incoming.data.subscriptionInitialPosition", SubscriptionInitialPosition.Earliest)
                .with("mp.messaging.incoming.data.schema", "INT32")
                // Tracks the in-flight messages, which rebuilds them.
                .with("mp.messaging.incoming.data.graceful-shutdown", true);
    }

    @ApplicationScoped
    public static class MessageConsumer {

        private final List<PulsarIncomingMessage<Integer>> received = new CopyOnWriteArrayList<>();

        @Incoming("data")
        public CompletionStage<Void> consume(PulsarIncomingMessage<Integer> message) {
            received.add(message);
            return message.ack();
        }

        public List<PulsarIncomingMessage<Integer>> received() {
            return received;
        }
    }

    @ApplicationScoped
    public static class BatchConsumer {

        private final List<PulsarBatchMessage<Integer>> batches = new CopyOnWriteArrayList<>();
        private final List<Integer> received = new CopyOnWriteArrayList<>();

        @Incoming("data")
        public CompletionStage<Void> consume(PulsarBatchMessage<Integer> batch) {
            batches.add(batch);
            for (PulsarMessage<Integer> message : batch) {
                received.add(message.getPayload());
            }
            return batch.ack();
        }

        public List<PulsarBatchMessage<Integer>> batches() {
            return batches;
        }

        public List<Integer> received() {
            return received;
        }
    }

    @ApplicationScoped
    public static class ExactlyOnceProcessor {

        @Inject
        @Channel("transactional-producer")
        PulsarTransactions<Integer> transaction;

        private final List<PulsarIncomingMessage<Integer>> received = new CopyOnWriteArrayList<>();

        @Incoming("data")
        Uni<Void> process(PulsarIncomingMessage<Integer> message) {
            received.add(message);
            return transaction.withTransaction(message, emitter -> {
                emitter.send(PulsarMessage.of(message.getPayload(), message.getKey()));
                return Uni.createFrom().voidItem();
            });
        }

        public List<PulsarIncomingMessage<Integer>> received() {
            return received;
        }
    }
}
