package io.smallrye.reactive.messaging.pulsar;

import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.junit.jupiter.api.Test;

public class ProducerConsumerTest extends PulsarBaseTest {

    @Test
    void testConsumer() throws PulsarClientException {
        System.out.println("creating producer");
        Producer<String> producer = client.newProducer(Schema.STRING)
                .producerName("test-producer")
                .topic(topic)
                .create();

        System.out.println("sending messages");
        send(producer, 5, String::valueOf);

        System.out.println("creating consumer");
        Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("test-" + topic)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .consumerName("test-consumer")
                .subscribe();

        List<String> received = new CopyOnWriteArrayList<>();
        receive(consumer, 5, message -> {
            try {
                System.out.println("Message received: " + message);
                received.add(message.getValue());
                consumer.acknowledge(message);
            } catch (Exception e) {
                consumer.negativeAcknowledge(message);
            }
        });

        await().until(() -> received.size() >= 5);
    }
}
