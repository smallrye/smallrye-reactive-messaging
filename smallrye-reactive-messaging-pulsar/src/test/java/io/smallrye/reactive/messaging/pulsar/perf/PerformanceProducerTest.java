package io.smallrye.reactive.messaging.pulsar.perf;

import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.LongAdder;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.OnOverflow;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.pulsar.PulsarConnector;
import io.smallrye.reactive.messaging.pulsar.TestTags;
import io.smallrye.reactive.messaging.pulsar.base.WeldTestBase;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

@Tag(TestTags.PERFORMANCE)
public class PerformanceProducerTest extends WeldTestBase {

    private static final int COUNT = 100_000;
    private static final int TIMEOUT_IN_MINUTES = 1;

    @Test
    public void testDefault() throws PulsarAdminException, PulsarClientException {
        String topic = UUID.randomUUID().toString();
        admin.topics().createPartitionedTopic(topic, 10);

        List<Integer> messages = new CopyOnWriteArrayList<>();
        receive(client.newConsumer(Schema.INT32)
                .subscriptionName(topic + "-consumer")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .topic(topic)
                .subscribe(), COUNT, m -> messages.add(m.getValue()));

        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.outgoing.pulsar.connector", PulsarConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.pulsar.serviceUrl", serviceUrl)
                .with("mp.messaging.outgoing.pulsar.tracing-enabled", false)
                .with("mp.messaging.outgoing.pulsar.topic", topic)
                .with("mp.messaging.outgoing.pulsar.schema", "INT32")
                .with("mp.messaging.outgoing.pulsar.numIoThreads", 16)
                .with("mp.messaging.outgoing.pulsar.connectionsPerBroker", 8);

        GeneratorBean bean = runApplication(config, GeneratorBean.class);
        await().until(this::isReady);
        await().until(this::isAlive);
        long begin = System.currentTimeMillis();
        bean.run();
        await()
                .atMost(Duration.ofMinutes(TIMEOUT_IN_MINUTES))
                .until(() -> bean.count() == COUNT);
        long end = System.currentTimeMillis();

        // Wait until all the messages are read.
        await().until(() -> messages.size() >= COUNT);

        long duration = end - begin;
        System.out.println("Time " + duration + " ms");
        double speed = (COUNT * 1.0) / (duration / 1000.0);
        System.out.println(speed + " messages/ms");
    }

    @Test
    public void testDefaultNoWait() throws PulsarAdminException, PulsarClientException {
        String topic = UUID.randomUUID().toString();
        admin.topics().createPartitionedTopic(topic, 10);

        List<Integer> messages = new CopyOnWriteArrayList<>();
        receive(client.newConsumer(Schema.INT32)
                .subscriptionName(topic + "-consumer")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .topic(topic)
                .subscribe(), COUNT, m -> messages.add(m.getValue()));

        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.outgoing.pulsar.connector", PulsarConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.pulsar.serviceUrl", serviceUrl)
                .with("mp.messaging.outgoing.pulsar.tracing-enabled", false)
                .with("mp.messaging.outgoing.pulsar.topic", topic)
                .with("mp.messaging.outgoing.pulsar.schema", "INT32")
                .with("mp.messaging.outgoing.pulsar.waitForWriteCompletion", false)
                .with("mp.messaging.outgoing.pulsar.numIoThreads", 16)
                .with("mp.messaging.outgoing.pulsar.connectionsPerBroker", 8);

        GeneratorBean bean = runApplication(config, GeneratorBean.class);
        await().until(this::isReady);
        await().until(this::isAlive);
        long begin = System.currentTimeMillis();
        bean.run();
        await()
                .atMost(Duration.ofMinutes(TIMEOUT_IN_MINUTES))
                .until(() -> bean.count() == COUNT);
        long end = System.currentTimeMillis();

        // Wait until all the messages are read.
        await().until(() -> messages.size() >= COUNT);

        long duration = end - begin;
        System.out.println("Time " + duration + " ms");
        double speed = (COUNT * 1.0) / (duration / 1000.0);
        System.out.println(speed + " messages/ms");
    }

    @Test
    public void testWithoutBackPressure() throws PulsarAdminException, PulsarClientException {
        String topic = UUID.randomUUID().toString();
        admin.topics().createPartitionedTopic(topic, 10);

        List<Integer> messages = new CopyOnWriteArrayList<>();
        receive(client.newConsumer(Schema.INT32)
                .subscriptionName(topic + "-consumer")
                .topic(topic)
                .subscribe(), COUNT, m -> messages.add(m.getValue()));

        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.outgoing.pulsar.connector", PulsarConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.pulsar.serviceUrl", serviceUrl)
                .with("mp.messaging.outgoing.pulsar.tracing-enabled", false)
                .with("mp.messaging.outgoing.pulsar.topic", topic)
                .with("mp.messaging.outgoing.pulsar.maxPendingMessages", 0L)
                .with("mp.messaging.outgoing.pulsar.schema", "INT32")
                .with("mp.messaging.outgoing.pulsar.numIoThreads", 16)
                .with("mp.messaging.outgoing.pulsar.connectionsPerBroker", 8);

        GeneratorBean bean = runApplication(config, GeneratorBean.class);
        await().until(this::isReady);
        await().until(this::isAlive);
        long begin = System.currentTimeMillis();
        bean.run();
        await()
                .atMost(Duration.ofMinutes(TIMEOUT_IN_MINUTES))
                .until(() -> bean.count() == COUNT);
        long end = System.currentTimeMillis();

        // Wait until all the messages are read.
        await().until(() -> messages.size() >= COUNT);

        long duration = end - begin;
        System.out.println("Time " + duration + " ms");
        double speed = (COUNT * 1.0) / (duration / 1000.0);
        System.out.println(speed + " messages/ms");
    }

    @Test
    public void testWithoutBackPressureAndNoWait() throws PulsarAdminException, PulsarClientException {
        String topic = UUID.randomUUID().toString();
        admin.topics().createPartitionedTopic(topic, 10);

        List<Integer> messages = new CopyOnWriteArrayList<>();
        receive(client.newConsumer(Schema.INT32)
                .subscriptionName(topic + "-consumer")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .topic(topic)
                .subscribe(), COUNT, m -> messages.add(m.getValue()));

        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.outgoing.pulsar.connector", PulsarConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.pulsar.serviceUrl", serviceUrl)
                .with("mp.messaging.outgoing.pulsar.tracing-enabled", false)
                .with("mp.messaging.outgoing.pulsar.topic", topic)
                .with("mp.messaging.outgoing.pulsar.maxPendingMessages", 0L)
                .with("mp.messaging.outgoing.pulsar.waitForWriteCompletion", false)
                .with("mp.messaging.outgoing.pulsar.schema", "INT32")
                .with("mp.messaging.outgoing.pulsar.numIoThreads", 16)
                .with("mp.messaging.outgoing.pulsar.connectionsPerBroker", 8);

        GeneratorBean bean = runApplication(config, GeneratorBean.class);
        await().until(this::isReady);
        await().until(this::isAlive);
        long begin = System.currentTimeMillis();
        bean.run();
        await()
                .atMost(Duration.ofMinutes(TIMEOUT_IN_MINUTES))
                .until(() -> bean.count() == COUNT);
        long end = System.currentTimeMillis();

        // Wait until all the messages are read.
        await().until(() -> messages.size() >= COUNT);

        long duration = end - begin;
        System.out.println("Time " + duration + " ms");
        double speed = (COUNT * 1.0) / (duration / 1000.0);
        System.out.println(speed + " messages/ms");
    }

    @Test
    public void testWithoutBackPressureAndIdempotence() throws PulsarAdminException, PulsarClientException {
        String topic = UUID.randomUUID().toString();
        admin.namespaces().setDeduplicationStatus("public/default", true);
        admin.topics().createPartitionedTopic(topic, 10);

        List<Integer> messages = new CopyOnWriteArrayList<>();
        receive(client.newConsumer(Schema.INT32)
                .subscriptionName(topic + "-consumer")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .topic(topic)
                .subscribe(), COUNT, m -> messages.add(m.getValue()));

        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.outgoing.pulsar.connector", PulsarConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.pulsar.serviceUrl", serviceUrl)
                .with("mp.messaging.outgoing.pulsar.tracing-enabled", false)
                .with("mp.messaging.outgoing.pulsar.topic", topic)
                .with("mp.messaging.outgoing.pulsar.maxPendingMessages", 0L)
                .with("mp.messaging.outgoing.pulsar.waitForWriteCompletion", false)
                .with("mp.messaging.outgoing.pulsar.schema", "INT32")
                .with("mp.messaging.outgoing.pulsar.numIoThreads", 16)
                .with("mp.messaging.outgoing.pulsar.connectionsPerBroker", 8);

        GeneratorBean bean = runApplication(config, GeneratorBean.class);
        await().until(this::isReady);
        await().until(this::isAlive);
        long begin = System.currentTimeMillis();
        bean.run();
        await()
                .atMost(Duration.ofMinutes(TIMEOUT_IN_MINUTES))
                .until(() -> bean.count() == COUNT);
        long end = System.currentTimeMillis();

        // Wait until all the messages are read.
        await().until(() -> messages.size() >= COUNT);

        long duration = end - begin;
        System.out.println("Time " + duration + " ms");
        double speed = (COUNT * 1.0) / (duration / 1000.0);
        System.out.println(speed + " messages/ms");
    }

    @ApplicationScoped
    public static class GeneratorBean {

        private final LongAdder counter = new LongAdder();

        @Inject
        @Channel("pulsar")
        @OnOverflow(bufferSize = COUNT, value = OnOverflow.Strategy.BUFFER)
        Emitter<Integer> emitter;

        public void run() {
            for (int i = 0; i < COUNT; i++) {
                int v = i;
                Message<Integer> message = Message.of(v, () -> {
                    counter.increment();
                    return CompletableFuture.completedFuture(null);
                });
                emitter.send(message);
            }
        }

        public long count() {
            return counter.sum();
        }

    }

}
