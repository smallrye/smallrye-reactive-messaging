package io.smallrye.reactive.messaging.pulsar.health;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Flow;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.pulsar.PulsarConnector;
import io.smallrye.reactive.messaging.pulsar.base.WeldTestBase;
import io.smallrye.reactive.messaging.pulsar.fault.PulsarFailStop;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class HealthCheckTest extends WeldTestBase {

    public static final int NUMBER_OF_MESSAGES = 10;

    @Test
    public void testHealthOfProducingApplication() throws PulsarClientException {
        runApplication(configProducingBean(), ProducingBean.class);

        await().until(this::isStarted);
        await().until(this::isReady);
        await().until(this::isAlive);

        List<org.apache.pulsar.client.api.Message<Integer>> messages = new CopyOnWriteArrayList<>();
        receive(client.newConsumer(Schema.INT32)
                .consumerName("test-consumer")
                .subscriptionName("subscription")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .topic(topic)
                .subscribe(), NUMBER_OF_MESSAGES, messages::add);

        await().until(() -> messages.size() == 10);
        HealthReport startup = getHealth().getStartup();
        HealthReport liveness = getHealth().getLiveness();
        HealthReport readiness = getHealth().getReadiness();

        assertThat(startup.isOk()).isTrue();
        assertThat(liveness.isOk()).isTrue();
        assertThat(readiness.isOk()).isTrue();
        assertThat(startup.getChannels()).hasSize(1);
        assertThat(liveness.getChannels()).hasSize(1);
        assertThat(readiness.getChannels()).hasSize(1);
        assertThat(liveness.getChannels().get(0).getChannel()).isEqualTo("output");
    }

    @Test
    public void testHealthOfConsumingApplication() throws PulsarClientException {
        LazyConsumingBean bean = runApplication(configConsumerBean(), LazyConsumingBean.class);

        await().until(this::isStarted);
        await().until(this::isReady);
        await().until(this::isAlive);

        send(client.newProducer(Schema.INT32)
                .topic(topic)
                .create(), NUMBER_OF_MESSAGES, i -> i);

        Multi<Integer> channel = bean.getChannel();
        channel
                .select().first(10)
                .collect().asList()
                .await().atMost(Duration.ofSeconds(10));

        HealthReport startup = getHealth().getStartup();
        HealthReport liveness = getHealth().getLiveness();
        HealthReport readiness = getHealth().getReadiness();

        assertThat(startup.isOk()).isTrue();
        assertThat(liveness.isOk()).isTrue();
        assertThat(readiness.isOk()).isTrue();
        assertThat(startup.getChannels()).hasSize(1);
        assertThat(liveness.getChannels()).hasSize(1);
        assertThat(readiness.getChannels()).hasSize(1);
        assertThat(liveness.getChannels().get(0).getChannel()).isEqualTo("input");
    }

    @Test
    public void testHealthOfConsumingApplicationWithFailure() throws PulsarClientException {
        addBeans(PulsarFailStop.Factory.class);
        runApplication(configConsumerBean()
                .with("mp.messaging.incoming.input.failure-strategy", "fail"), ConsumingWithFailureBean.class);

        await().until(this::isStarted);
        await().until(this::isReady);
        await().until(this::isAlive);

        send(client.newProducer(Schema.INT32)
                .topic(topic)
                .create(), NUMBER_OF_MESSAGES, i -> i);

        await().until(() -> !isAlive());

        await().untilAsserted(() -> {
            HealthReport startup = getHealth().getStartup();
            HealthReport liveness = getHealth().getLiveness();
            HealthReport readiness = getHealth().getReadiness();

            assertThat(startup.isOk()).isFalse();
            assertThat(readiness.isOk()).isFalse();
            assertThat(liveness.isOk()).isFalse();
            assertThat(startup.getChannels()).hasSize(1);
            assertThat(readiness.getChannels()).hasSize(2);
            assertThat(liveness.getChannels()).hasSize(2);

            assertThat(liveness.getChannels())
                    .anySatisfy(channelInfo -> assertThat(channelInfo.getChannel()).isEqualTo("input"))
                    .allSatisfy(channelInfo -> assertThat(channelInfo.isOk()).isFalse());
        });
    }

    private MapBasedConfig configProducingBean() {
        return new MapBasedConfig()
                .with("mp.messaging.outgoing.output.connector", PulsarConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.output.serviceUrl", serviceUrl)
                .with("mp.messaging.outgoing.output.topic", topic)
                .with("mp.messaging.outgoing.output.schema", "INT32");
    }

    private MapBasedConfig configConsumerBean() {
        return new MapBasedConfig()
                .with("mp.messaging.incoming.input.connector", PulsarConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.input.serviceUrl", serviceUrl)
                .with("mp.messaging.incoming.input.topic", topic)
                .with("mp.messaging.incoming.input.schema", "INT32")
                .with("mp.messaging.incoming.input.topic.subscriptionInitialPosition", "Earliest");
    }

    @ApplicationScoped
    public static class ProducingBean {

        @Incoming("data")
        @Outgoing("output")
        @Acknowledgment(Acknowledgment.Strategy.MANUAL)
        public Message<Integer> process(Message<Integer> input) {
            return Message.of(input.getPayload() + 1, input::ack);
        }

        @Outgoing("data")
        public Flow.Publisher<Integer> source() {
            return Multi.createFrom().range(0, 10);
        }

    }

    @ApplicationScoped
    public static class LazyConsumingBean {

        @Inject
        @Channel("input")
        Multi<Integer> channel;

        public Multi<Integer> getChannel() {
            return channel;
        }
    }

    @ApplicationScoped
    public static class ConsumingWithFailureBean {

        @Incoming("input")
        public void consume(Integer payload) {
            if (payload > 5) {
                throw new IllegalArgumentException("boom");
            }
        }
    }

}
