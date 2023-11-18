package io.smallrye.reactive.messaging.mqtt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorLiteral;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.mqtt.messages.MqttPublishMessage;

public class ConcurrentProcessorTest extends MqttTestBase {

    private WeldContainer container;

    private String topic;
    private String clientId;

    private MapBasedConfig dataconfig() {
        return new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", MqttConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.host", address)
                .with("mp.messaging.incoming.data.port", port)
                .with("mp.messaging.incoming.data.client-id", clientId)
                .with("mp.messaging.incoming.data.qos", 1)
                .with("mp.messaging.incoming.data.concurrency", 3)
                .with("mp.messaging.incoming.data$1.topic", topic + "-1")
                .with("mp.messaging.incoming.data$2.topic", topic + "-2")
                .with("mp.messaging.incoming.data$3.topic", topic + "-3");
    }

    private void produceMessages() {
        CountDownLatch latch = new CountDownLatch(3);
        AtomicInteger counter = new AtomicInteger(0);
        usage.produceIntegers(topic + "-1", 4, latch::countDown, counter::getAndIncrement);
        usage.produceIntegers(topic + "-2", 3, latch::countDown, counter::getAndIncrement);
        usage.produceIntegers(topic + "-3", 3, latch::countDown, counter::getAndIncrement);
        try {
            latch.await(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private <T> T runApplication(MapBasedConfig config, Class<T> beanClass) {
        Weld weld = baseWeld(config);
        weld.addBeanClass(beanClass);
        container = weld.initialize();

        waitUntilReady(container);
        return container.getBeanManager().createInstance().select(beanClass).get();
    }

    public void waitUntilReady(WeldContainer container) {
        MqttConnector connector = container.select(MqttConnector.class,
                ConnectorLiteral.of(MqttConnector.CONNECTOR_NAME)).get();
        await().until(() -> connector.getReadiness().isOk());
    }

    @BeforeEach
    public void setupTopicName() {
        topic = UUID.randomUUID().toString();
        clientId = UUID.randomUUID().toString();
    }

    @AfterEach
    public void cleanup() {
        if (container != null) {
            container.close();
        }
        Clients.clear();
    }

    @Test
    public void testConcurrentConsumer() {
        MyConsumerBean bean = runApplication(dataconfig(), MyConsumerBean.class);
        waitUntilReady(container);

        List<Integer> list = bean.getResults();
        assertThat(list).isEmpty();

        produceMessages();
        await().untilAsserted(() -> {
            assertThat(bean.getResults()).hasSize(10);
            assertThat(bean.getPerThread().keySet()).hasSize(3);
        });
        assertThat(bean.getResults()).containsExactlyInAnyOrder(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    }

    @Test
    public void testConcurrentProcessor() {
        MyProcessorBean bean = runApplication(dataconfig(), MyProcessorBean.class);
        waitUntilReady(container);

        List<Integer> list = bean.getResults();
        assertThat(list).isEmpty();

        produceMessages();
        await().untilAsserted(() -> {
            assertThat(bean.getResults()).hasSize(10);
            assertThat(bean.getPerThread().keySet()).hasSize(3);
        });
        assertThat(bean.getResults()).containsExactlyInAnyOrder(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    }

    @Test
    public void testConcurrentStreamTransformer() {
        MyStreamTransformerBean bean = runApplication(dataconfig(), MyStreamTransformerBean.class);
        waitUntilReady(container);

        List<Integer> list = bean.getResults();
        assertThat(list).isEmpty();

        produceMessages();
        await().untilAsserted(() -> {
            assertThat(bean.getResults()).hasSize(10);
            assertThat(bean.getPerThread().keySet()).hasSize(3);
        });
        assertThat(bean.getResults()).containsExactlyInAnyOrder(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    }

    @Test
    public void testConcurrentStreamInjectingBean() {
        MyChannelInjectingBean bean = runApplication(dataconfig(), MyChannelInjectingBean.class);
        bean.process();
        waitUntilReady(container);

        List<Integer> list = bean.getResults();
        assertThat(list).isEmpty();

        produceMessages();
        await().untilAsserted(() -> {
            assertThat(bean.getResults()).hasSize(10);
            assertThat(bean.getPerThread().keySet()).hasSize(3);
        });
        assertThat(bean.getResults()).containsExactlyInAnyOrder(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    }

    @ApplicationScoped
    public static class MyConsumerBean {

        private final List<Integer> list = new CopyOnWriteArrayList<>();
        private final Map<Thread, List<Integer>> perThread = new ConcurrentHashMap<>();

        @Incoming("data")
        public Uni<Void> process(MqttPublishMessage input) {
            int value = Integer.parseInt(input.payload().toString());
            int next = value + 1;
            perThread.computeIfAbsent(Thread.currentThread(), t -> new CopyOnWriteArrayList<>()).add(next);
            list.add(next);
            return Uni.createFrom().voidItem().onItem().delayIt().by(Duration.ofMillis(100));
        }

        public List<Integer> getResults() {
            return list;
        }

        public Map<Thread, List<Integer>> getPerThread() {
            return perThread;
        }
    }

    @ApplicationScoped
    public static class MyProcessorBean {

        private final List<Integer> list = new CopyOnWriteArrayList<>();
        private final Map<Thread, List<Integer>> perThread = new ConcurrentHashMap<>();

        @Incoming("data")
        @Outgoing("sink")
        @Acknowledgment(Acknowledgment.Strategy.MANUAL)
        public Uni<Message<Integer>> process(Message<String> input) {
            int value = Integer.parseInt(input.getPayload());
            int next = value + 1;
            perThread.computeIfAbsent(Thread.currentThread(), t -> new CopyOnWriteArrayList<>()).add(next);
            return Uni.createFrom().item(Message.of(next, input::ack))
                    .onItem().delayIt().by(Duration.ofMillis(100));
        }

        @Incoming("sink")
        public void sink(int val) {
            list.add(val);
        }

        public List<Integer> getResults() {
            return list;
        }

        public Map<Thread, List<Integer>> getPerThread() {
            return perThread;
        }
    }

    @ApplicationScoped
    public static class MyStreamTransformerBean {

        private final List<Integer> list = new CopyOnWriteArrayList<>();
        private final Map<Thread, List<Integer>> perThread = new ConcurrentHashMap<>();

        @Incoming("data")
        @Outgoing("sink")
        public Multi<Message<Integer>> process(Multi<Message<String>> multi) {
            return multi.onItem()
                    .transformToUniAndConcatenate(input -> {
                        int value = Integer.parseInt(input.getPayload());
                        int next = value + 1;
                        perThread.computeIfAbsent(Thread.currentThread(), t -> new CopyOnWriteArrayList<>()).add(next);
                        return Uni.createFrom().item(Message.of(next, input::ack))
                                .onItem().delayIt().by(Duration.ofMillis(100));
                    });
        }

        @Incoming("sink")
        public void sink(int val) {
            list.add(val);
        }

        public List<Integer> getResults() {
            return list;
        }

        public Map<Thread, List<Integer>> getPerThread() {
            return perThread;
        }
    }

    @ApplicationScoped
    public static class MyChannelInjectingBean {

        private final List<Integer> list = new CopyOnWriteArrayList<>();
        private final Map<Thread, List<Integer>> perThread = new ConcurrentHashMap<>();

        @Channel("data")
        @Inject
        Multi<Message<String>> multi;

        public void process() {
            multi.onItem()
                    .transformToUniAndConcatenate(input -> {
                        int value = Integer.parseInt(input.getPayload());
                        int next = value + 1;
                        list.add(next);
                        perThread.computeIfAbsent(Thread.currentThread(), t -> new CopyOnWriteArrayList<>()).add(next);
                        return Uni.createFrom().completionStage(input::ack)
                                .onItem().delayIt().by(Duration.ofMillis(100));
                    })
                    .subscribe().with(__ -> {
                    });
        }

        public List<Integer> getResults() {
            return list;
        }

        public Map<Thread, List<Integer>> getPerThread() {
            return perThread;
        }
    }

}
