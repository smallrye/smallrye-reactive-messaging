package io.smallrye.reactive.messaging.pulsar;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.pulsar.client.api.BatcherBuilder;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.pulsar.base.WeldTestBase;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class ConcurrentProcessorTest extends WeldTestBase {

    private MapBasedConfig dataconfig() {
        return new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", PulsarConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.serviceUrl", serviceUrl)
                .with("mp.messaging.incoming.data.topic", topic)
                .with("mp.messaging.incoming.data.schema", "INT32")
                .with("mp.messaging.incoming.data.subscriptionInitialPosition", SubscriptionInitialPosition.Earliest)
                .with("mp.messaging.incoming.data.subscriptionName", topic + "-subscription")
                .with("mp.messaging.incoming.data.subscriptionType", SubscriptionType.Key_Shared)
                .with("mp.messaging.incoming.data.concurrency", 3);
    }

    @Test
    public void testConcurrentConsumer() {
        MyConsumerBean bean = runApplication(dataconfig(), MyConsumerBean.class);

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

        List<Integer> list = bean.getResults();
        assertThat(list).isEmpty();
        bean.process();

        produceMessages();
        await().untilAsserted(() -> {
            assertThat(bean.getResults()).hasSize(10);
            assertThat(bean.getPerThread().keySet()).hasSize(3);
        });
        assertThat(bean.getResults()).containsExactlyInAnyOrder(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    }

    private void produceMessages() {
        try {
            send(client.newProducer(Schema.INT32)
                    .producerName(topic + "-producer")
                    .batcherBuilder(BatcherBuilder.KEY_BASED)
                    .topic(topic)
                    .create(), 10, (i, p) -> p.newMessage().key(getKey(i)).value(i));
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    private static String getKey(int v) {
        switch (v % 3) {
            case 0:
                return "foo";
            case 1:
                return "bar";
            case 2:
                return "qux";
        }
        return null;
    }

    @ApplicationScoped
    public static class MyConsumerBean {

        private final List<Integer> list = new CopyOnWriteArrayList<>();
        private final Map<Thread, List<Integer>> perThread = new ConcurrentHashMap<>();

        @Incoming("data")
        public Uni<Void> process(org.apache.pulsar.client.api.Message<Integer> input) {
            int value = input.getValue();
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
        public Uni<Message<Integer>> process(PulsarMessage<Integer> input) {
            int value = input.getPayload();
            int next = value + 1;
            perThread.computeIfAbsent(Thread.currentThread(), t -> new CopyOnWriteArrayList<>()).add(next);
            return Uni.createFrom().item(input.withPayload(next))
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
        public Multi<Message<Integer>> process(Multi<PulsarMessage<Integer>> multi) {
            return multi.onItem()
                    .transformToUniAndConcatenate(input -> {
                        int value = input.getPayload();
                        int next = value + 1;
                        perThread.computeIfAbsent(Thread.currentThread(), t -> new CopyOnWriteArrayList<>()).add(next);
                        return Uni.createFrom().item(input.withPayload(next))
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
