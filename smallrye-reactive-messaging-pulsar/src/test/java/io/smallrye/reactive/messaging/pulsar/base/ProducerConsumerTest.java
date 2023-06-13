package io.smallrye.reactive.messaging.pulsar.base;

import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.junit.jupiter.api.Test;

public class ProducerConsumerTest extends PulsarBaseTest {

    @Test
    void testConsumer() throws PulsarClientException {
        Producer<String> producer = client.newProducer(Schema.STRING)
                .producerName("test-producer")
                .topic(topic)
                .create();

        send(producer, 5, String::valueOf);

        Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("test-" + topic)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .consumerName("test-consumer")
                .subscribe();

        List<String> received = new CopyOnWriteArrayList<>();
        receive(consumer, 5, message -> {
            try {
                received.add(message.getValue());
                consumer.acknowledge(message);
            } catch (Exception e) {
                consumer.negativeAcknowledge(message);
            }
        });

        await().until(() -> received.size() >= 5);
    }

    @Test
    void testBatchConsumer() throws PulsarClientException {
        Producer<String> producer = client.newProducer(Schema.STRING)
                .producerName("test-producer")
                .enableBatching(true)
                .batchingMaxMessages(10)
                .topic(topic)
                .create();

        send(producer, 100, String::valueOf);

        Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("test-" + topic)
                .subscriptionType(SubscriptionType.Failover)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .consumerName("test-consumer")
                .subscribe();

        List<String> received = new CopyOnWriteArrayList<>();
        receiveBatch(consumer, 100, messages -> {
            try {
                for (Message<String> message : messages) {
                    received.add(message.getValue());
                    consumer.acknowledge(message);
                }
            } catch (Exception e) {
                consumer.negativeAcknowledge(messages);
            }
        });

        await().until(() -> received.size() >= 100);
    }
}
