package io.smallrye.reactive.messaging.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.kafka.converters.ConsumerRecordConverter;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.core.impl.cpu.CpuCoreSensor;

public class ConcurrentProcessorTest extends KafkaCompanionTestBase {

    private MapBasedConfig dataconfig() {
        String groupId = UUID.randomUUID().toString();
        return kafkaConfig("mp.messaging.incoming.data")
                .with("group.id", groupId)
                .with("topic", topic)
                .with("concurrency", 3)
                .with("failure-strategy", "dead-letter-queue")
                .with("auto.offset.reset", "earliest")
                .with("value.deserializer", IntegerDeserializer.class.getName());
    }

    private void produceMessages() {
        int expected = 10;
        companion.produceIntegers().usingGenerator(i -> {
            int p = i % 3;
            return new ProducerRecord<>(topic, p, Integer.toString(p), i);
        }, expected).awaitCompletion(Duration.ofMinutes(1));
    }

    @Test
    public void testConcurrentConsumer() {
        addBeans(ConsumerRecordConverter.class);
        companion.topics().createAndWait(topic, 3);

        produceMessages();
        MyConsumerBean bean = runApplication(dataconfig(), MyConsumerBean.class);

        List<Integer> list = bean.getResults();
        assertThat(list).isEmpty();

        await().untilAsserted(() -> {
            assertThat(bean.getResults())
                    .hasSizeGreaterThanOrEqualTo(10)
                    .contains(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
            assertThat(bean.getPerThread().keySet()).hasSizeGreaterThanOrEqualTo(getMaxNumberOfEventLoop(3));
        });
    }

    @Test
    public void testConcurrentProcessor() {
        companion.topics().createAndWait(topic, 3);

        produceMessages();
        MyProcessorBean bean = runApplication(dataconfig(), MyProcessorBean.class);

        List<Integer> list = bean.getResults();
        assertThat(list).isEmpty();

        await().untilAsserted(() -> {
            assertThat(bean.getResults())
                    .hasSizeGreaterThanOrEqualTo(10)
                    .contains(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
            assertThat(bean.getPerThread().keySet()).hasSizeGreaterThanOrEqualTo(getMaxNumberOfEventLoop(3));
        });
    }

    @Test
    public void testConcurrentStreamTransformer() {
        companion.topics().createAndWait(topic, 3);

        produceMessages();
        MyStreamTransformerBean bean = runApplication(dataconfig(), MyStreamTransformerBean.class);

        List<Integer> list = bean.getResults();
        assertThat(list).isEmpty();

        await().untilAsserted(() -> {
            assertThat(bean.getResults())
                    .hasSizeGreaterThanOrEqualTo(10)
                    .contains(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
            assertThat(bean.getPerThread().keySet()).hasSizeGreaterThanOrEqualTo(getMaxNumberOfEventLoop(3));
        });
    }

    @Test
    public void testConcurrentStreamInjectingBean() {
        companion.topics().createAndWait(topic, 3);

        produceMessages();
        MyChannelInjectingBean bean = runApplication(dataconfig(), MyChannelInjectingBean.class);

        List<Integer> list = bean.getResults();
        assertThat(list).isEmpty();
        bean.process();

        await().untilAsserted(() -> {
            assertThat(bean.getResults())
                    .hasSizeGreaterThanOrEqualTo(10)
                    .containsExactlyInAnyOrder(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
            assertThat(bean.getPerThread().keySet()).hasSizeGreaterThanOrEqualTo(getMaxNumberOfEventLoop(3));
        });
    }

    @ApplicationScoped
    public static class MyConsumerBean {

        private final List<Integer> list = new CopyOnWriteArrayList<>();
        private final Map<Thread, List<Integer>> perThread = new ConcurrentHashMap<>();

        @Incoming("data")
        public Uni<Void> process(ConsumerRecord<String, Integer> record) {
            int value = record.value();
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
        public Uni<Message<Integer>> process(Message<Integer> input) {
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
        public Multi<Message<Integer>> process(Multi<Message<Integer>> multi) {
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

    public int getMaxNumberOfEventLoop(int expected) {
        // On Github Actions, only one event loop is created.
        int cpus = CpuCoreSensor.availableProcessors();
        // For some reason when Github Actions has 4 cores it'll still run on 1 event loop thread
        if (cpus <= 4) {
            return 1;
        }
        return Math.min(expected, cpus / 2);
    }

}
