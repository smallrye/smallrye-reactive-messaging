package io.smallrye.reactive.messaging.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.smallrye.reactive.messaging.annotations.Blocking;
import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;
import io.smallrye.reactive.messaging.kafka.base.KafkaTestBase;
import io.vertx.mutiny.kafka.client.consumer.KafkaConsumer;

public class SourceBackPressureWithBrokerTest extends KafkaTestBase {

    @Test
    void testPauseResume() {
        String topic = UUID.randomUUID().toString();
        ConsumptionBean bean = run(myKafkaSourceConfig(topic), ConsumptionBean.class);
        KafkaClientService clients = get(KafkaClientService.class);
        KafkaConsumer<String, String> consumer = clients.getConsumer("data");
        assertThat(consumer).isNotNull();
        List<String> list = bean.list();
        assertThat(list).isEmpty();
        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceStrings(10, null,
                () -> new ProducerRecord<>(topic, "" + counter.getAndIncrement()))).start();

        await().until(() -> bean.request(2));
        await().until(() -> list.size() >= 2);
        await().until(() -> !consumer.pausedAndAwait().isEmpty());

        await().until(() -> bean.request(8));
        await()
                .pollInterval(Duration.ofMillis(10))
                .until(() -> consumer.paused().await().atMost(Duration.ofSeconds(3)).isEmpty());
        await().atMost(2, TimeUnit.MINUTES).until(() -> list.size() == 10);
        await().until(() -> consumer.pausedAndAwait().isEmpty());
    }

    @Test
    void testPauseResumeButRequestBeforePausing() {
        String topic = UUID.randomUUID().toString();
        ConsumptionBean bean = run(myKafkaSourceConfig(topic), ConsumptionBean.class);
        KafkaClientService clients = get(KafkaClientService.class);
        KafkaConsumer<String, String> consumer = clients.getConsumer("data");
        assertThat(consumer).isNotNull();

        List<String> list = bean.list();
        assertThat(list).isEmpty();
        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceStrings(10, null,
                () -> new ProducerRecord<>(topic, "" + counter.getAndIncrement()))).start();

        await().until(() -> bean.request(2));
        await().until(() -> list.size() >= 2);
        await().until(() -> consumer.pausedAndAwait().size() > 0);
        await().until(() -> bean.request(3));
        await()
            .pollInterval(Duration.ofMillis(10))
            .until(() -> consumer.pausedAndAwait().size() == 0);
        await().until(() -> bean.request(5));
        await()
            .pollInterval(Duration.ofMillis(10))
            .until(() -> consumer.pausedAndAwait().size() == 0);
        await().atMost(2, TimeUnit.MINUTES).until(() -> list.size() == 10);

        await()
                .pollInterval(Duration.ofMillis(10))
                .until(() -> consumer.paused().await().atMost(Duration.ofSeconds(1)).isEmpty());
    }

    @Test
    void testPauseResumeWithBlockingConsumption() {
        String topic = UUID.randomUUID().toString();
        BlockingBean bean = run(myKafkaSourceConfig(topic), BlockingBean.class);
        KafkaClientService clients = get(KafkaClientService.class);
        KafkaConsumer<String, String> consumer = clients.getConsumer("data");
        assertThat(consumer).isNotNull();
        List<String> list = bean.list();
        assertThat(list).isEmpty();
        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceStrings(5, null,
                () -> new ProducerRecord<>(topic, "" + counter.getAndIncrement()))).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> list.size() >= 2);

        await().until(() -> !consumer.pausedAndAwait().isEmpty());

        await()
                .pollInterval(Duration.ofMillis(10))
                .until(() -> consumer.paused().await().atMost(Duration.ofSeconds(1)).isEmpty());
        await().atMost(2, TimeUnit.MINUTES).until(() -> list.size() == 5);
    }

    private KafkaMapBasedConfig myKafkaSourceConfig(String topic) {
        KafkaMapBasedConfig.Builder builder = KafkaMapBasedConfig.builder("mp.messaging.incoming.data");

        builder.put("value.deserializer", StringDeserializer.class.getName());
        builder.put("enable.auto.commit", "false");
        builder.put("auto.offset.reset", "earliest");
        builder.put("topic", topic);
        builder.put("pause-after-inactivity", 1);

        return builder.build();
    }

    private <T> T run(KafkaMapBasedConfig config, Class<T> bean) {
        addBeans(bean);
        runApplication(config);
        return get(bean);
    }

    @SuppressWarnings("ReactiveStreamsSubscriberImplementation")
    @ApplicationScoped
    static class ConsumptionBean {

        private volatile Subscription subscription;
        private final List<String> list = new ArrayList<>();

        @Incoming("data")
        Subscriber<String> consume() {
            return new Subscriber<String>() {
                @Override
                public void onSubscribe(Subscription s) {
                    subscription = s;
                }

                @Override
                public void onNext(String s) {
                    list.add(s);
                }

                @Override
                public void onError(Throwable t) {

                }

                @Override
                public void onComplete() {

                }
            };
        }

        public boolean request(int i) {
            if (subscription != null) {
                subscription.request(i);
                return true;
            }
            return false;
        }

        public List<String> list() {
            return list;
        }
    }

    @ApplicationScoped
    static class BlockingBean {

        private final List<String> list = new ArrayList<>();

        @Incoming("data")
        @Outgoing("out")
        @Blocking
        public String process(String s) throws InterruptedException {
            Thread.sleep(2000);
            return s.toUpperCase();
        }

        @Incoming("out")
        void consume(String s) {
            list.add(s);
        }

        public List<String> list() {
            return list;
        }
    }

}
