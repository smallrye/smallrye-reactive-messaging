package io.smallrye.reactive.messaging.pulsar.transactions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
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
import org.eclipse.microprofile.reactive.messaging.OnOverflow;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.pulsar.PulsarConnector;
import io.smallrye.reactive.messaging.pulsar.PulsarIncomingMessage;
import io.smallrye.reactive.messaging.pulsar.PulsarMessage;
import io.smallrye.reactive.messaging.pulsar.base.WeldTestBase;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class ExactlyOnceProcessingTest extends WeldTestBase {

    String inTopic;
    String outTopic;

    @Test
    void testExactlyOnceProcessor() throws PulsarAdminException, PulsarClientException {
        this.inTopic = UUID.randomUUID().toString();
        admin.topics().createPartitionedTopic(inTopic, 3);
        this.outTopic = UUID.randomUUID().toString();
        admin.topics().createPartitionedTopic(outTopic, 3);
        int numberOfRecords = 10;
        MapBasedConfig config = new MapBasedConfig(producerConfig());
        config.putAll(consumerConfig());
        ExactlyOnceProcessor application = runApplication(config, ExactlyOnceProcessor.class);

        send(client.newProducer(Schema.INT32)
                .producerName("test-producer")
                .topic(this.inTopic)
                .create(), numberOfRecords, (i, producer) -> producer.newMessage().value(i).key("k0" + i));

        List<Integer> list = new CopyOnWriteArrayList<>();
        receive(client.newConsumer(Schema.INT32)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .consumerName("test-consumer")
                .subscriptionName("test-subscription")
                .topic(this.outTopic)
                .subscribe(), numberOfRecords, m -> list.add(m.getValue()));

        await().atMost(1, TimeUnit.MINUTES).untilAsserted(() -> assertThat(list)
                .containsAll(IntStream.range(0, numberOfRecords).boxed().collect(Collectors.toList()))
                .doesNotHaveDuplicates());
    }

    @ApplicationScoped
    public static class ExactlyOnceProcessor {

        @Inject
        @Channel("transactional-producer")
        PulsarTransactions<Integer> transaction;

        @Incoming("exactly-once-consumer")
        Uni<Void> process(PulsarIncomingMessage<Integer> record) {
            return transaction.withTransaction(record, emitter -> {
                emitter.send(PulsarMessage.of(record.getPayload(), record.getKey()));
                return Uni.createFrom().voidItem();
            });
        }
    }

    @Test
    void testExactlyOnceProcessorWithProcessingError() throws PulsarAdminException, PulsarClientException {
        this.inTopic = UUID.randomUUID().toString();
        admin.topics().createPartitionedTopic(inTopic, 3);
        this.outTopic = UUID.randomUUID().toString();
        admin.topics().createPartitionedTopic(outTopic, 3);
        int numberOfRecords = 10;
        MapBasedConfig config = new MapBasedConfig(producerConfig());
        config.putAll(consumerConfig());
        ExactlyOnceProcessorWithProcessingError application = runApplication(config,
                ExactlyOnceProcessorWithProcessingError.class);

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

        await().untilAsserted(() -> assertThat(application.getProcessed())
                .containsAll(IntStream.range(0, numberOfRecords).boxed().collect(Collectors.toList()))
                .doesNotHaveDuplicates());
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
                .with("mp.messaging.outgoing.transactional-producer.schema", "INT32");
    }

    private MapBasedConfig consumerConfig() {
        return baseConfig()
                .with("mp.messaging.incoming.exactly-once-consumer.connector", PulsarConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.exactly-once-consumer.serviceUrl", serviceUrl)
                .with("mp.messaging.incoming.exactly-once-consumer.topic", inTopic)
                .with("mp.messaging.incoming.exactly-once-consumer.subscriptionInitialPosition", "Earliest")
                .with("mp.messaging.incoming.exactly-once-consumer.enableTransaction", true)
                .with("mp.messaging.incoming.exactly-once-consumer.negativeAckRedeliveryDelayMicros", 100)
                .with("mp.messaging.incoming.exactly-once-consumer.schema", "INT32");
    }

    @ApplicationScoped
    public static class ExactlyOnceProcessorWithProcessingError {

        @Inject
        @Channel("transactional-producer")
        @OnOverflow(value = OnOverflow.Strategy.BUFFER, bufferSize = 256)
        PulsarTransactions<Integer> transaction;

        volatile boolean error = true;

        List<Integer> processed = new CopyOnWriteArrayList<>();

        @Incoming("exactly-once-consumer")
        Uni<Void> process(PulsarIncomingMessage<Integer> record) {
            return transaction.withTransactionAndAck(record, emitter -> {
                if (error && record.getPayload() == 5) {
                    error = false;
                    throw new IllegalArgumentException("Error on first try");
                }
                processed.add(record.getPayload());
                emitter.send(PulsarMessage.of(record.getPayload(), record.getKey()));
                return Uni.createFrom().voidItem();
            });
        }

        public List<Integer> getProcessed() {
            return processed;
        }
    }
}
