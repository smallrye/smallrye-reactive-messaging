package io.smallrye.reactive.messaging.rabbitmq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Test;

import com.rabbitmq.client.AMQP;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class ConcurrentProcessorTest extends WeldTestBase {

    private MapBasedConfig dataconfig() {
        return commonConfig()
                .with("mp.messaging.incoming.data.connector", RabbitMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.queue.durable", true)
                .with("mp.messaging.incoming.data.queue.name", "(server.auto)")
                .with("mp.messaging.incoming.data.exchange.name", exchange)
                .with("mp.messaging.incoming.data.exchange.type", "direct")
                .with("mp.messaging.incoming.data.max-outstanding-messages", 1)
                .with("mp.messaging.incoming.data.concurrency", 3)
                .with("mp.messaging.incoming.data$1.routing-keys", "foo")
                .with("mp.messaging.incoming.data$2.routing-keys", "bar")
                .with("mp.messaging.incoming.data$3.routing-keys", "qux");
    }

    private void produceMessages() {
        AtomicInteger counter = new AtomicInteger(0);
        usage.produce(exchange, null, "foo", 4, counter::getAndIncrement,
                new AMQP.BasicProperties.Builder().contentType("text/plain").headers(Map.of("key", "foo")).build());
        usage.produce(exchange, null, "bar", 3, counter::getAndIncrement,
                new AMQP.BasicProperties.Builder().contentType("text/plain").headers(Map.of("key", "bar")).build());
        usage.produce(exchange, null, "qux", 3, counter::getAndIncrement,
                new AMQP.BasicProperties.Builder().contentType("text/plain").headers(Map.of("key", "qux")).build());
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
        weld.addBeanClass(MyChannelInjectingBean.class);
        dataconfig().write();
        container = weld.initialize();
        MyChannelInjectingBean bean = get(MyChannelInjectingBean.class);
        bean.process();
        await().until(() -> isRabbitMQConnectorAlive(container));
        await().until(() -> isRabbitMQConnectorReady(container));

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
        public Uni<Void> process(String input) {
            int value = Integer.parseInt(input);
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
        public Uni<Message<Integer>> process(IncomingRabbitMQMessage<String> input) {
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
        public Multi<Message<Integer>> process(Multi<IncomingRabbitMQMessage<String>> multi) {
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

        @Inject
        @Channel("data")
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
