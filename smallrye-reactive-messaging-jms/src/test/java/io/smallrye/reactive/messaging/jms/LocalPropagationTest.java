package io.smallrye.reactive.messaging.jms;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSProducer;
import jakarta.jms.ObjectMessage;
import jakarta.jms.Queue;

import org.apache.activemq.artemis.jms.client.ActiveMQJMSConnectionFactory;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.smallrye.common.vertx.ContextLocals;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.reactive.messaging.annotations.Blocking;
import io.smallrye.reactive.messaging.annotations.Broadcast;
import io.smallrye.reactive.messaging.annotations.Merge;
import io.smallrye.reactive.messaging.providers.locals.LocalContextMetadata;
import io.smallrye.reactive.messaging.support.JmsTestBase;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.mutiny.core.Vertx;

public class LocalPropagationTest extends JmsTestBase {

    private JMSContext jms;
    private ActiveMQJMSConnectionFactory factory;
    private ExecutorService executor;
    private Queue queue;

    @BeforeEach
    public void init() {
        factory = new ActiveMQJMSConnectionFactory(
                "tcp://localhost:61616",
                null, null);
        jms = factory.createContext();
        executor = Executors.newFixedThreadPool(3);
        queue = jms.createQueue("queue-one");
    }

    @AfterEach
    public void close() {
        jms.close();
        factory.close();
        executor.shutdown();
    }

    private MapBasedConfig dataconfig() {
        return new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", JmsConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.destination", "queue-one")
                .with("mp.messaging.incoming.data.tracing.enabled", false);
    }

    private void produceIntegers() {
        JMSProducer producer = jms.createProducer();
        for (int i = 1; i < 6; i++) {
            ObjectMessage message = jms.createObjectMessage(i);
            producer.send(queue, message);
        }
    }

    private <T> T runApplication(MapBasedConfig config, Class<T> beanClass) {
        addConfig(config);
        WeldContainer container = deploy(beanClass);
        return container.getBeanManager().createInstance().select(beanClass).get();
    }

    @Test
    public void testLinearPipeline() {
        LinearPipeline bean = runApplication(dataconfig(), LinearPipeline.class);
        produceIntegers();

        await().atMost(20, TimeUnit.SECONDS).until(() -> {
            List<Integer> results = bean.getResults();
            System.out.println(results);
            return results.size() >= 5;
        });
        assertThat(bean.getResults()).containsExactly(2, 3, 4, 5, 6);
    }

    @Test
    public void testPipelineWithABlockingStage() {
        PipelineWithABlockingStage bean = runApplication(dataconfig(), PipelineWithABlockingStage.class);
        produceIntegers();

        await().atMost(20, TimeUnit.SECONDS).until(() -> {
            List<Integer> results = bean.getResults();
            System.out.println(results);
            return results.size() >= 5;
        });
        assertThat(bean.getResults()).containsExactly(2, 3, 4, 5, 6);

    }

    @Test
    public void testPipelineWithAnUnorderedBlockingStage() {
        PipelineWithAnUnorderedBlockingStage bean = runApplication(dataconfig(), PipelineWithAnUnorderedBlockingStage.class);
        produceIntegers();

        await().atMost(20, TimeUnit.SECONDS).until(() -> {
            List<Integer> results = bean.getResults();
            System.out.println(results);
            return results.size() >= 5;
        });
        assertThat(bean.getResults()).containsExactlyInAnyOrder(2, 3, 4, 5, 6);

    }

    @Test
    public void testPipelineWithMultipleBlockingStages() {
        PipelineWithMultipleBlockingStages bean = runApplication(dataconfig(), PipelineWithMultipleBlockingStages.class);
        produceIntegers();

        await().atMost(20, TimeUnit.SECONDS).until(() -> {
            List<Integer> results = bean.getResults();
            System.out.println(results);
            return results.size() >= 5;
        });
        assertThat(bean.getResults()).containsExactlyInAnyOrder(2, 3, 4, 5, 6);
    }

    @Test
    public void testPipelineWithBroadcastAndMerge() {
        PipelineWithBroadcastAndMerge bean = runApplication(dataconfig(), PipelineWithBroadcastAndMerge.class);
        produceIntegers();

        await().atMost(20, TimeUnit.SECONDS).until(() -> {
            List<Integer> results = bean.getResults();
            System.out.println(results);
            return results.size() >= 10;
        });
        assertThat(bean.getResults()).hasSize(10).contains(2, 3, 4, 5, 6);
    }

    @Test
    public void testLinearPipelineWithAckOnCustomThread() {
        LinearPipelineWithAckOnCustomThread bean = runApplication(dataconfig(), LinearPipelineWithAckOnCustomThread.class);
        produceIntegers();

        await().atMost(20, TimeUnit.SECONDS).until(() -> {
            List<Integer> results = bean.getResults();
            System.out.println(results);
            return results.size() >= 5;
        });
        assertThat(bean.getResults()).containsExactly(2, 3, 4, 5, 6);

    }

    @Test
    public void testPipelineWithAnAsyncStage() {
        PipelineWithAnAsyncStage bean = runApplication(dataconfig(), PipelineWithAnAsyncStage.class);
        produceIntegers();

        await().atMost(20, TimeUnit.SECONDS).until(() -> {
            List<Integer> results = bean.getResults();
            System.out.println(results);
            return results.size() >= 5;
        });
        assertThat(bean.getResults()).containsExactly(2, 3, 4, 5, 6);

    }

    @ApplicationScoped
    public static class LinearPipeline {

        private final List<Integer> list = new CopyOnWriteArrayList<>();
        private final Set<String> uuids = new ConcurrentHashSet<>();

        @Incoming("data")
        @Outgoing("process")
        @Acknowledgment(Acknowledgment.Strategy.MANUAL)
        public Message<Integer> process(Message<Integer> input) {
            String value = UUID.randomUUID().toString();
            Vertx.currentContext().putLocal("uuid", value);
            Vertx.currentContext().putLocal("input", input.getPayload());

            return input.withPayload(input.getPayload() + 1);
        }

        @Incoming("process")
        @Outgoing("after-process")
        public Integer handle(int payload) {
            String uuid = Vertx.currentContext().getLocal("uuid");
            assertThat(uuid).isNotNull();

            assertThat(uuids.add(uuid)).isTrue();

            int p = Vertx.currentContext().getLocal("input");
            assertThat(p + 1).isEqualTo(payload);
            return payload;
        }

        @Incoming("after-process")
        @Outgoing("sink")
        public Integer afterProcess(int payload) {
            String uuid = Vertx.currentContext().getLocal("uuid");
            assertThat(uuid).isNotNull();

            int p = Vertx.currentContext().getLocal("input");
            assertThat(p + 1).isEqualTo(payload);
            return payload;
        }

        @Incoming("sink")
        public void sink(int val) {
            String uuid = Vertx.currentContext().getLocal("uuid");
            assertThat(uuid).isNotNull();

            int p = Vertx.currentContext().getLocal("input");
            assertThat(p + 1).isEqualTo(val);
            list.add(val);
        }

        public List<Integer> getResults() {
            return list;
        }
    }

    @ApplicationScoped
    public static class LinearPipelineWithAckOnCustomThread {

        private final List<Integer> list = new CopyOnWriteArrayList<>();
        private final Set<String> uuids = new ConcurrentHashSet<>();

        private final Executor executor = Executors.newFixedThreadPool(4);

        @Incoming("data")
        @Outgoing("process")
        @Acknowledgment(Acknowledgment.Strategy.MANUAL)
        public Message<Integer> process(Message<Integer> input) {
            String value = UUID.randomUUID().toString();

            assertThat((String) Vertx.currentContext().getLocal("uuid")).isNull();
            Vertx.currentContext().putLocal("uuid", value);
            Vertx.currentContext().putLocal("input", input.getPayload());

            return input.withPayload(input.getPayload() + 1)
                    .withAck(() -> {
                        CompletableFuture<Void> cf = new CompletableFuture<>();
                        executor.execute(() -> {
                            cf.complete(null);
                        });
                        return cf;
                    });
        }

        @Incoming("process")
        @Outgoing("after-process")
        public Integer handle(int payload) {
            try {
                String uuid = Vertx.currentContext().getLocal("uuid");
                assertThat(uuid).isNotNull();

                assertThat(uuids.add(uuid)).isTrue();

                int p = Vertx.currentContext().getLocal("input");
                assertThat(p + 1).isEqualTo(payload);
            } catch (Exception e) {
                e.printStackTrace();
            }
            return payload;
        }

        @Incoming("after-process")
        @Outgoing("sink")
        public Integer afterProcess(int payload) {
            try {
                String uuid = Vertx.currentContext().getLocal("uuid");
                assertThat(uuid).isNotNull();

                int p = Vertx.currentContext().getLocal("input");
                assertThat(p + 1).isEqualTo(payload);
            } catch (Exception e) {
                e.printStackTrace();
            }
            return payload;
        }

        @Incoming("sink")
        public void sink(int val) {
            String uuid = Vertx.currentContext().getLocal("uuid");
            assertThat(uuid).isNotNull();

            int p = Vertx.currentContext().getLocal("input");
            assertThat(p + 1).isEqualTo(val);
            list.add(val);
        }

        public List<Integer> getResults() {
            return list;
        }
    }

    @ApplicationScoped
    public static class PipelineWithABlockingStage {

        private final List<Integer> list = new CopyOnWriteArrayList<>();
        private final Set<String> uuids = new ConcurrentHashSet<>();

        @Incoming("data")
        @Outgoing("process")
        @Acknowledgment(Acknowledgment.Strategy.MANUAL)
        public Message<Integer> process(Message<Integer> input) {
            String value = UUID.randomUUID().toString();

            assertThat((String) Vertx.currentContext().getLocal("uuid")).isNull();
            Vertx.currentContext().putLocal("uuid", value);
            Vertx.currentContext().putLocal("input", input.getPayload());

            assertThat(input.getMetadata(LocalContextMetadata.class)).isPresent();

            return input.withPayload(input.getPayload() + 1);
        }

        @Incoming("process")
        @Outgoing("after-process")
        @Blocking
        public Integer handle(int payload) {
            String uuid = Vertx.currentContext().getLocal("uuid");
            assertThat(uuid).isNotNull();

            assertThat(uuids.add(uuid)).isTrue();

            int p = Vertx.currentContext().getLocal("input");
            assertThat(p + 1).isEqualTo(payload);
            return payload;
        }

        @Incoming("after-process")
        @Outgoing("sink")
        public Integer afterProcess(int payload) {
            String uuid = Vertx.currentContext().getLocal("uuid");
            assertThat(uuid).isNotNull();

            int p = Vertx.currentContext().getLocal("input");
            assertThat(p + 1).isEqualTo(payload);
            return payload;
        }

        @Incoming("sink")
        public void sink(int val) {
            String uuid = Vertx.currentContext().getLocal("uuid");
            assertThat(uuid).isNotNull();

            int p = Vertx.currentContext().getLocal("input");
            assertThat(p + 1).isEqualTo(val);
            list.add(val);
        }

        public List<Integer> getResults() {
            return list;
        }
    }

    @ApplicationScoped
    public static class PipelineWithAnUnorderedBlockingStage {

        private final List<Integer> list = new CopyOnWriteArrayList<>();
        private final Set<String> uuids = new ConcurrentHashSet<>();

        @Incoming("data")
        @Outgoing("process")
        @Acknowledgment(Acknowledgment.Strategy.MANUAL)
        public Message<Integer> process(Message<Integer> input) {
            String value = UUID.randomUUID().toString();

            assertThat((String) Vertx.currentContext().getLocal("uuid")).isNull();
            Vertx.currentContext().putLocal("uuid", value);
            Vertx.currentContext().putLocal("input", input.getPayload());

            assertThat(input.getMetadata(LocalContextMetadata.class)).isPresent();

            return input.withPayload(input.getPayload() + 1);
        }

        private final Random random = new Random();

        @Incoming("process")
        @Outgoing("after-process")
        @Blocking(ordered = false)
        public Integer handle(int payload) throws InterruptedException {
            Thread.sleep(random.nextInt(10));
            String uuid = Vertx.currentContext().getLocal("uuid");
            assertThat(uuid).isNotNull();
            assertThat(uuids.add(uuid)).isTrue();

            int p = Vertx.currentContext().getLocal("input");
            assertThat(p + 1).isEqualTo(payload);
            return payload;
        }

        @Incoming("after-process")
        @Outgoing("sink")
        public Integer afterProcess(int payload) {
            String uuid = Vertx.currentContext().getLocal("uuid");
            assertThat(uuid).isNotNull();

            int p = Vertx.currentContext().getLocal("input");
            assertThat(p + 1).isEqualTo(payload);
            return payload;
        }

        @Incoming("sink")
        public void sink(int val) {
            String uuid = Vertx.currentContext().getLocal("uuid");
            assertThat(uuid).isNotNull();

            int p = Vertx.currentContext().getLocal("input");
            assertThat(p + 1).isEqualTo(val);
            list.add(val);
        }

        public List<Integer> getResults() {
            return list;
        }
    }

    @ApplicationScoped
    public static class PipelineWithMultipleBlockingStages {

        private final List<Integer> list = new CopyOnWriteArrayList<>();
        private final Set<String> uuids = new ConcurrentHashSet<>();

        @Incoming("data")
        @Outgoing("process")
        @Acknowledgment(Acknowledgment.Strategy.MANUAL)
        public Message<Integer> process(Message<Integer> input) {
            System.out.println("Processing " + input.getPayload());
            String value = UUID.randomUUID().toString();
            assertThat((String) Vertx.currentContext().getLocal("uuid")).isNull();
            Vertx.currentContext().putLocal("uuid", value);
            Vertx.currentContext().putLocal("input", input.getPayload());

            assertThat(input.getMetadata(LocalContextMetadata.class)).isPresent();

            return input.withPayload(input.getPayload() + 1);
        }

        private final Random random = new Random();

        @Incoming("process")
        @Outgoing("second-blocking")
        @Blocking(ordered = false)
        public Integer handle(int payload) throws InterruptedException {
            Thread.sleep(random.nextInt(10));
            String uuid = Vertx.currentContext().getLocal("uuid");
            assertThat(uuid).isNotNull();
            assertThat(uuids.add(uuid)).isTrue();

            int p = Vertx.currentContext().getLocal("input");
            assertThat(p + 1).isEqualTo(payload);
            return payload;
        }

        @Incoming("second-blocking")
        @Outgoing("after-process")
        @Blocking
        public Integer handle2(int payload) throws InterruptedException {
            Thread.sleep(random.nextInt(10));
            String uuid = Vertx.currentContext().getLocal("uuid");
            assertThat(uuid).isNotNull();

            int p = Vertx.currentContext().getLocal("input");
            assertThat(p + 1).isEqualTo(payload);
            return payload;
        }

        @Incoming("after-process")
        @Outgoing("sink")
        public Integer afterProcess(int payload) {
            String uuid = Vertx.currentContext().getLocal("uuid");
            assertThat(uuid).isNotNull();

            int p = Vertx.currentContext().getLocal("input");
            assertThat(p + 1).isEqualTo(payload);
            return payload;
        }

        @Incoming("sink")
        public void sink(int val) {
            String uuid = Vertx.currentContext().getLocal("uuid");
            assertThat(uuid).isNotNull();

            int p = Vertx.currentContext().getLocal("input");
            assertThat(p + 1).isEqualTo(val);
            list.add(val);
        }

        public List<Integer> getResults() {
            return list;
        }
    }

    @ApplicationScoped
    public static class PipelineWithBroadcastAndMerge {

        private final List<Integer> list = new CopyOnWriteArrayList<>();
        private final Set<String> branch1 = new ConcurrentHashSet<>();
        private final Set<String> branch2 = new ConcurrentHashSet<>();

        @Incoming("data")
        @Outgoing("process")
        @Acknowledgment(Acknowledgment.Strategy.MANUAL)
        @Broadcast(2)
        public Message<Integer> process(Message<Integer> input) {
            String value = UUID.randomUUID().toString();

            assertThat((String) Vertx.currentContext().getLocal("uuid")).isNull();
            Vertx.currentContext().putLocal("uuid", value);
            Vertx.currentContext().putLocal("input", input.getPayload());

            assertThat(input.getMetadata(LocalContextMetadata.class)).isPresent();

            return input.withPayload(input.getPayload() + 1);
        }

        private final Random random = new Random();

        @Incoming("process")
        @Outgoing("after-process")
        public Integer branch1(int payload) {
            String uuid = Vertx.currentContext().getLocal("uuid");
            assertThat(uuid).isNotNull();
            assertThat(branch1.add(uuid)).isTrue();

            int p = Vertx.currentContext().getLocal("input");
            assertThat(p + 1).isEqualTo(payload);
            return payload;
        }

        @Incoming("process")
        @Outgoing("after-process")
        public Integer branch2(int payload) {
            String uuid = Vertx.currentContext().getLocal("uuid");
            assertThat(uuid).isNotNull();
            assertThat(branch2.add(uuid)).isTrue();

            int p = Vertx.currentContext().getLocal("input");
            assertThat(p + 1).isEqualTo(payload);
            return payload;
        }

        @Incoming("after-process")
        @Outgoing("sink")
        @Merge
        public Integer afterProcess(int payload) {
            String uuid = Vertx.currentContext().getLocal("uuid");
            assertThat(uuid).isNotNull();

            int p = Vertx.currentContext().getLocal("input");
            assertThat(p + 1).isEqualTo(payload);
            return payload;
        }

        @Incoming("sink")
        public void sink(int val) {
            String uuid = Vertx.currentContext().getLocal("uuid");
            assertThat(uuid).isNotNull();

            int p = Vertx.currentContext().getLocal("input");
            assertThat(p + 1).isEqualTo(val);
            list.add(val);
        }

        public List<Integer> getResults() {
            return list;
        }
    }

    @ApplicationScoped
    public static class PipelineWithAnAsyncStage {

        private final List<Integer> list = new CopyOnWriteArrayList<>();
        private final Set<String> uuids = new ConcurrentHashSet<>();

        @Incoming("data")
        @Outgoing("process")
        @Acknowledgment(Acknowledgment.Strategy.MANUAL)
        public Message<Integer> process(Message<Integer> input) {
            String value = UUID.randomUUID().toString();
            assertThat((String) ContextLocals.get("uuid", null)).isNull();
            ContextLocals.put("uuid", value);
            ContextLocals.put("input", input.getPayload());

            assertThat(input.getMetadata(LocalContextMetadata.class)).isPresent();

            return input.withPayload(input.getPayload() + 1);
        }

        @Incoming("process")
        @Outgoing("after-process")
        public Uni<Integer> handle(int payload) {
            String uuid = ContextLocals.get("uuid", null);
            assertThat(uuid).isNotNull();

            assertThat(uuids.add(uuid)).isTrue();

            int p = ContextLocals.get("input", null);
            assertThat(p + 1).isEqualTo(payload);
            return Uni.createFrom().item(() -> payload)
                    .runSubscriptionOn(Infrastructure.getDefaultExecutor());
        }

        @Incoming("after-process")
        @Outgoing("sink")
        public Integer afterProcess(int payload) {
            String uuid = ContextLocals.get("uuid", null);
            assertThat(uuid).isNotNull();

            int p = ContextLocals.get("input", null);
            assertThat(p + 1).isEqualTo(payload);
            return payload;
        }

        @Incoming("sink")
        public void sink(int val) {
            String uuid = ContextLocals.get("uuid", null);
            assertThat(uuid).isNotNull();

            int p = ContextLocals.get("input", null);
            assertThat(p + 1).isEqualTo(val);
            list.add(val);
        }

        public List<Integer> getResults() {
            return list;
        }
    }

}
