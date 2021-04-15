package io.smallrye.reactive.messaging.kafka.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.annotations.Blocking;
import io.smallrye.reactive.messaging.kafka.KafkaClientService;
import io.smallrye.reactive.messaging.kafka.KafkaConsumer;
import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;
import io.smallrye.reactive.messaging.kafka.base.KafkaTestBase;

public class SourceBackPressureWithBrokerTest extends KafkaTestBase {

    @Test
    void testPauseResume() {
        String topic = UUID.randomUUID().toString();
        ConsumptionBean bean = run(myKafkaSourceConfig(topic), ConsumptionBean.class);
        KafkaClientService clients = get(KafkaClientService.class);
        KafkaConsumer<String, String> consumer = clients.getConsumer("data");
        assertThat(consumer).isNotNull();
        bean.run();
        List<String> list = bean.list();
        assertThat(list).isEmpty();
        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceStrings(10, null,
                () -> new ProducerRecord<>(topic, "" + counter.getAndIncrement()))).start();

        await().until(() -> bean.request(2));
        await().until(() -> list.size() >= 2);
        await().until(() -> !consumer.paused().await().atMost(Duration.ofSeconds(3)).isEmpty());

        await().until(() -> bean.request(8));
        await().atMost(2, TimeUnit.MINUTES).until(() -> list.size() == 10);
        await().until(() -> !consumer.paused().await().atMost(Duration.ofSeconds(3)).isEmpty());
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

        bean.run();
        await().until(() -> bean.request(2));

        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceStrings(10, null,
                () -> new ProducerRecord<>(topic, "" + counter.getAndIncrement()))).start();

        await().until(() -> list.size() == 2);
        await().until(() -> consumer.paused().await().atMost(Duration.ofSeconds(3)).size() > 0);
        await().until(() -> bean.request(3));
        await().until(() -> bean.request(5));
        await().atMost(2, TimeUnit.MINUTES).until(() -> list.size() == 10);
        await().until(() -> consumer.paused().await().atMost(Duration.ofSeconds(3)).size() > 0);

        bean.request(Long.MAX_VALUE);
        await()
                .pollInterval(Duration.ofMillis(10))
                .until(() -> consumer.paused().await().atMost(Duration.ofSeconds(3)).isEmpty());
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

        await().until(() -> !consumer.paused().await().atMost(Duration.ofSeconds(3)).isEmpty());

        await()
                .pollInterval(Duration.ofMillis(10))
                .until(() -> consumer.paused().await().indefinitely().isEmpty());
        await().atMost(2, TimeUnit.MINUTES).until(() -> list.size() == 5);
    }

    private KafkaMapBasedConfig myKafkaSourceConfig(String topic) {
        KafkaMapBasedConfig.Builder builder = KafkaMapBasedConfig.builder("mp.messaging.incoming.data");

        builder.put("value.deserializer", StringDeserializer.class.getName());
        builder.put("enable.auto.commit", "false");
        builder.put("auto.offset.reset", "earliest");
        builder.put("topic", topic);

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
        private final List<String> list = new CopyOnWriteArrayList<>();

        @Inject
        @Channel("data")
        Multi<Message<String>> stream;

        public void run() {
            stream.subscribe().withSubscriber(new Subscriber<Message<String>>() {
                @Override
                public void onSubscribe(Subscription s) {
                    subscription = s;
                }

                @Override
                public void onNext(Message<String> s) {
                    s.ack();
                    list.add(s.getPayload());
                }

                @Override
                public void onError(Throwable t) {

                }

                @Override
                public void onComplete() {

                }
            });
        }

        public synchronized boolean request(long i) {
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

        @SuppressWarnings("unused")
        @Incoming("out")
        void consume(String s) {
            list.add(s);
        }

        public List<String> list() {
            return list;
        }
    }

}
