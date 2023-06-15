package io.smallrye.reactive.messaging.pulsar.transactions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;

import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.BatchReceivePolicy;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.OnOverflow;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.pulsar.PulsarConnector;
import io.smallrye.reactive.messaging.pulsar.PulsarIncomingBatchMessage;
import io.smallrye.reactive.messaging.pulsar.PulsarMessage;
import io.smallrye.reactive.messaging.pulsar.TestTags;
import io.smallrye.reactive.messaging.pulsar.base.WeldTestBase;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class ExactlyOnceProcessingBatchTest extends WeldTestBase {

    String inTopic;
    String outTopic;

    @Test
    void testExactlyOnceProcessor() throws PulsarAdminException, PulsarClientException {
        this.inTopic = UUID.randomUUID().toString();
        admin.topics().createPartitionedTopic(inTopic, 3);
        this.outTopic = UUID.randomUUID().toString();
        admin.topics().createPartitionedTopic(outTopic, 3);
        int numberOfRecords = 1000;
        MapBasedConfig config = new MapBasedConfig(producerConfig());
        config.putAll(consumerConfig());
        ExactlyOnceProcessor application = runApplication(config, ExactlyOnceProcessor.class);

        send(client.newProducer(Schema.INT32)
                .producerName("test-producer")
                .topic(this.inTopic)
                .create(), numberOfRecords, (i, producer) -> producer.newMessage().value(i).key("k-" + i));

        List<Integer> list = new CopyOnWriteArrayList<>();
        receive(client.newConsumer(Schema.INT32)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .consumerName("test-consumer")
                .subscriptionName("test-subscription")
                .topic(this.outTopic)
                .subscribe(), numberOfRecords, m -> list.add(m.getValue()));

        await().untilAsserted(() -> assertThat(list)
                .containsAll(IntStream.range(0, numberOfRecords).boxed().collect(Collectors.toList()))
                .doesNotHaveDuplicates());
    }

    @ApplicationScoped
    public static class ExactlyOnceProcessor {

        @Inject
        @Channel("transactional-producer")
        PulsarTransactions<Integer> transaction;

        @Incoming("exactly-once-consumer")
        Uni<Void> process(PulsarIncomingBatchMessage<Integer> batch) {
            return transaction.withTransactionAndAck(batch, emitter -> {
                for (PulsarMessage<Integer> record : batch) {
                    emitter.send(PulsarMessage.of(record.getPayload(), record.getKey()));
                }
                return Uni.createFrom().voidItem();
            });
        }
    }

    /**
     * TODO Disabled test
     * For exactly-once processing the broker needs to enable the Message deduplication with
     * broker config <code>brokerDeduplicationEnabled=true</code> and producer `sendTimeoutMs` needs to be `0`.
     *
     * However only this doesn't solve the issue of duplicated items.
     *
     * There is also batch index level acknowledgement to avoid duplicated items which can be enabled with,
     * The broker config <code>acknowledgmentAtBatchIndexLevelEnabled=true</code> and consumer config `batchIndexAckEnable` to
     * `true.
     *
     * There are still duplicate items delivered to the consumer batch after an transaction abort.
     */
    @Test
    @Disabled
    @Tag(TestTags.FLAKY)
    void testExactlyOnceProcessorWithProcessingError() throws PulsarAdminException, PulsarClientException {
        addBeans(ConsumerConfig.class);
        this.inTopic = UUID.randomUUID().toString();
        admin.topics().createPartitionedTopic(inTopic, 3);
        this.outTopic = UUID.randomUUID().toString();
        admin.topics().createPartitionedTopic(outTopic, 3);
        int numberOfRecords = 1000;
        MapBasedConfig config = new MapBasedConfig(producerConfig());
        config.putAll(consumerConfig());
        ExactlyOnceProcessorWithProcessingError app = runApplication(config, ExactlyOnceProcessorWithProcessingError.class);

        List<Integer> list = new CopyOnWriteArrayList<>();
        receiveBatch(client.newConsumer(Schema.INT32)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .consumerName("test-consumer")
                .subscriptionName("test-subscription")
                .topic(this.outTopic)
                .enableBatchIndexAcknowledgment(true)
                .subscribe()).subscribe().with(messages -> {
                    for (Message<Integer> message : messages) {
                        System.out.println(
                                "-received " + message.getSequenceId() + " - " + message.getKey() + ":" + message.getValue());
                        list.add(message.getValue());
                    }
                });

        send(client.newProducer(Schema.INT32)
                .producerName("test-producer")
                .topic(this.inTopic)
                .create(), numberOfRecords, (i, producer) -> producer.newMessage().sequenceId(i).value(i).key("k-" + i));

        await().untilAsserted(() -> assertThat(app.getProcessed())
                .containsAll(IntStream.range(0, numberOfRecords).boxed().collect(Collectors.toList())));

        await().untilAsserted(() -> assertThat(list)
                .containsAll(IntStream.range(0, numberOfRecords).boxed().collect(Collectors.toList()))
                .doesNotHaveDuplicates());
    }

    private MapBasedConfig producerConfig() {
        return baseConfig()
                .with("mp.messaging.outgoing.transactional-producer.connector", PulsarConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.transactional-producer.serviceUrl", serviceUrl)
                .with("mp.messaging.outgoing.transactional-producer.topic", outTopic)
                .with("mp.messaging.outgoing.transactional-producer.enableTransaction", true)
                .with("mp.messaging.outgoing.transactional-producer.sendTimeoutMs", 0)
                .with("mp.messaging.outgoing.transactional-producer.schema", "INT32");
    }

    private MapBasedConfig consumerConfig() {
        return baseConfig()
                .with("mp.messaging.incoming.exactly-once-consumer.sendTimeoutMs", 0)
                .with("mp.messaging.incoming.exactly-once-consumer.connector", PulsarConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.exactly-once-consumer.serviceUrl", serviceUrl)
                .with("mp.messaging.incoming.exactly-once-consumer.topic", inTopic)
                .with("mp.messaging.incoming.exactly-once-consumer.subscriptionInitialPosition", "Earliest")
                .with("mp.messaging.incoming.exactly-once-consumer.enableTransaction", true)
                .with("mp.messaging.incoming.exactly-once-consumer.negativeAckRedeliveryDelayMicros", 5000)
                .with("mp.messaging.incoming.exactly-once-consumer.batchIndexAckEnabled", true)
                .with("mp.messaging.incoming.exactly-once-consumer.schema", "INT32")
                .with("mp.messaging.incoming.exactly-once-consumer.batchReceive", true);
    }

    @ApplicationScoped
    public static class ConsumerConfig {

        @Produces
        @Identifier("exactly-once-consumer")
        ConsumerConfigurationData<Integer> data() {
            var data = new ConsumerConfigurationData<Integer>();
            data.setBatchReceivePolicy(BatchReceivePolicy.builder()
                    .maxNumMessages(200)
                    .maxNumBytes(1024 * 2)
                    .timeout(30, TimeUnit.MILLISECONDS)
                    .build());
            return data;
        }
    }

    @ApplicationScoped
    public static class ExactlyOnceProcessorWithProcessingError {

        @Inject
        @Channel("transactional-producer")
        @OnOverflow(value = OnOverflow.Strategy.BUFFER, bufferSize = 1024)
        PulsarTransactions<Integer> transaction;

        AtomicBoolean error = new AtomicBoolean(true);

        List<Integer> processed = new CopyOnWriteArrayList<>();

        @Incoming("exactly-once-consumer")
        Uni<Void> process(PulsarIncomingBatchMessage<Integer> batch) {
            return transaction.withTransactionAndAck(batch, emitter -> {
                for (PulsarMessage<Integer> record : batch) {
                    if (record.getPayload() == 700 && error.compareAndSet(true, false)) {
                        throw new IllegalArgumentException("Error on first try");
                    }
                    emitter.send(PulsarMessage.of(record.getPayload(), record.getKey()));
                }
                processed.addAll(batch.getPayload());
                return Uni.createFrom().voidItem();
            });
        }

        public List<Integer> getProcessed() {
            return processed;
        }
    }

    @ApplicationScoped
    public static class InvalidExactlyOnceProcessor {
        @Inject
        @Channel("transactional-producer")
        @OnOverflow(value = OnOverflow.Strategy.BUFFER, bufferSize = 256)
        PulsarTransactions<Integer> transaction;

        @Incoming("exactly-once-consumer")
        Uni<Void> process(PulsarIncomingBatchMessage<Integer> batch) {
            return transaction.withTransaction(batch, emitter -> {
                for (PulsarMessage<Integer> record : batch) {
                    emitter.send(PulsarMessage.of(record.getPayload(), record.getKey()));
                }
                return Uni.createFrom().voidItem();
            });
        }

    }
}
