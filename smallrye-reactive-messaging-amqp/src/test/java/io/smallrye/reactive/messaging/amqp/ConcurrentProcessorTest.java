package io.smallrye.reactive.messaging.amqp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.amqp.AmqpMessageBuilder;

public class ConcurrentProcessorTest extends AmqpBrokerTestBase {

    private final Weld weld = new Weld();

    private WeldContainer container;

    private String address;

    @BeforeEach
    public void initTopic(TestInfo testInfo) {
        String cn = testInfo.getTestClass().map(Class::getSimpleName).orElse(UUID.randomUUID().toString());
        String mn = testInfo.getTestMethod().map(Method::getName).orElse(UUID.randomUUID().toString());
        address = cn + "-" + mn + "-" + UUID.randomUUID().getMostSignificantBits();
    }

    @AfterEach
    public void cleanup() {
        if (container != null) {
            container.close();
        }
        // Release the config objects
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
    }

    private MapBasedConfig dataconfig() {
        return new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.address", address)
                .with("mp.messaging.incoming.data.durable", false)
                .with("mp.messaging.incoming.data.concurrency", 3)
                .with("mp.messaging.incoming.data$1.selector", "x='foo'")
                .with("mp.messaging.incoming.data$2.selector", "x='bar'")
                .with("mp.messaging.incoming.data$3.selector", "x='baz'")
                .with("mp.messaging.incoming.data.host", host)
                .with("mp.messaging.incoming.data.port", port)
                .with("amqp-username", username)
                .with("amqp-password", password);
    }

    private <T> T runApplication(MapBasedConfig config, Class<T> beanClass) {
        config.write();
        weld.addBeanClass(beanClass);
        container = weld.initialize();

        return container.getBeanManager().createInstance().select(beanClass).get();
    }

    @Test
    public void testConcurrentConsumer() {
        MyConsumerBean bean = runApplication(dataconfig(), MyConsumerBean.class);
        await().until(() -> isAmqpConnectorReady(container));
        await().until(() -> isAmqpConnectorAlive(container));

        List<Integer> list = bean.getResults();
        assertThat(list).isEmpty();

        produceMessages();
        await().untilAsserted(() -> {
            assertThat(bean.getResults()).hasSizeGreaterThanOrEqualTo(10);
            assertThat(bean.getPerThread().keySet()).hasSize(3);
        });
        assertThat(bean.getResults()).containsExactlyInAnyOrder(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    }

    @Test
    public void testConcurrentProcessor() {
        MyProcessorBean bean = runApplication(dataconfig(), MyProcessorBean.class);
        await().until(() -> isAmqpConnectorReady(container));
        await().until(() -> isAmqpConnectorAlive(container));

        List<Integer> list = bean.getResults();
        assertThat(list).isEmpty();

        produceMessages();
        await().untilAsserted(() -> {
            assertThat(bean.getResults()).hasSizeGreaterThanOrEqualTo(10);
            assertThat(bean.getPerThread().keySet()).hasSize(3);
        });
        assertThat(bean.getResults()).containsExactlyInAnyOrder(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    }

    @Test
    public void testConcurrentStreamTransformer() {
        MyStreamTransformerBean bean = runApplication(dataconfig(), MyStreamTransformerBean.class);
        await().until(() -> isAmqpConnectorReady(container));
        await().until(() -> isAmqpConnectorAlive(container));

        List<Integer> list = bean.getResults();
        assertThat(list).isEmpty();

        produceMessages();
        await().untilAsserted(() -> {
            assertThat(bean.getResults()).hasSizeGreaterThanOrEqualTo(10);
            assertThat(bean.getPerThread().keySet()).hasSize(3);
        });
        assertThat(bean.getResults()).containsExactlyInAnyOrder(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    }

    private void produceMessages() {
        AtomicInteger counter = new AtomicInteger();
        usage.produce(address, 10, () -> {
            int v = counter.getAndIncrement();
            AmqpMessageBuilder builder = io.vertx.mutiny.amqp.AmqpMessage.create()
                    .durable(false)
                    .ttl(10000)
                    .address(address)
                    .applicationProperties(new JsonObject().put("x", getX(v)))
                    .withIntegerAsBody(v);
            return builder.build();
        });
    }

    private static String getX(int v) {
        switch (v % 3) {
            case 0:
                return "foo";
            case 1:
                return "bar";
            case 2:
                return "baz";
        }
        return null;
    }

    @Test
    public void testConcurrentStreamInjectingBean() {
        MyChannelInjectingBean bean = runApplication(dataconfig(), MyChannelInjectingBean.class);
        bean.process();
        await().until(() -> isAmqpConnectorReady(container));
        await().until(() -> isAmqpConnectorAlive(container));

        List<Integer> list = bean.getResults();
        assertThat(list).isEmpty();

        produceMessages();
        await().untilAsserted(() -> {
            assertThat(bean.getResults()).hasSizeGreaterThanOrEqualTo(10);
            assertThat(bean.getPerThread().keySet()).hasSizeGreaterThanOrEqualTo(3);
        });
        assertThat(bean.getResults()).containsExactlyInAnyOrder(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    }

    @ApplicationScoped
    public static class MyConsumerBean {

        private final List<Integer> list = new CopyOnWriteArrayList<>();
        private final Map<Thread, List<Integer>> perThread = new ConcurrentHashMap<>();

        @Incoming("data")
        public Uni<Void> process(io.vertx.amqp.AmqpMessage input) {
            int value = input.bodyAsInteger();
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
        public Uni<Message<Integer>> process(AmqpMessage<Integer> input) {
            int value = input.getPayload();
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
        public Multi<Message<Integer>> process(Multi<AmqpMessage<Integer>> multi) {
            return multi.onItem()
                    .transformToUniAndConcatenate(input -> {
                        int value = input.getPayload();
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
        Multi<Message<Integer>> multi;

        public void process() {
            multi.onItem()
                    .transformToUniAndConcatenate(input -> {
                        int value = input.getPayload();
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
