package io.smallrye.reactive.messaging.rabbitmq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.providers.extension.HealthCenter;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class ConsumerBackPressureTest extends WeldTestBase {

    private MapBasedConfig getBaseConfig() {
        return commonConfig()
                .with("mp.messaging.outgoing.to-rabbitmq.connector", RabbitMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.to-rabbitmq.exchange.name", exchangeName)

                .with("mp.messaging.incoming.from-rabbitmq.connector", RabbitMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.from-rabbitmq.queue.name", queueName)
                .with("mp.messaging.incoming.from-rabbitmq.health-lazy-subscription", true)
                .with("mp.messaging.incoming.from-rabbitmq.queue.durable", true)

                .with("mp.messaging.incoming.from-rabbitmq.exchange.name", exchangeName);
    }

    @Test
    void testConsumerBackpressure() {
        addBeans(Publisher.class, Subscriber.class);
        runApplication(getBaseConfig());

        Subscriber subscriber = get(Subscriber.class);
        Publisher publisher = get(Publisher.class);
        HealthCenter health = get(HealthCenter.class);

        AssertSubscriber<Object> consumer = AssertSubscriber.create(0);
        subscriber.getMulti().subscribe().withSubscriber(consumer);

        await().untilAsserted(() -> assertThat(health.getLiveness().isOk()).isTrue());

        publisher.generate();

        AtomicInteger i = new AtomicInteger(1);
        while (consumer.getItems().size() < 10000) {
            consumer.request(100);
            await().until(() -> consumer.getItems().size() == 100 * i.get());
            i.getAndIncrement();
        }
        await().until(() -> consumer.getItems().size() == 10000);

    }

    @ApplicationScoped
    public static class Publisher {

        @Inject
        @Channel("to-rabbitmq")
        MutinyEmitter<String> emitter;

        public void generate() {
            for (int i = 0; i < 10000; i++) {
                emitter.sendAndAwait(Integer.toString(i));
            }
        }

    }

    @ApplicationScoped
    public static class Subscriber {

        @Inject
        @Channel("from-rabbitmq")
        Multi<String> multi;

        public Multi<String> getMulti() {
            return multi;
        }
    }

}
