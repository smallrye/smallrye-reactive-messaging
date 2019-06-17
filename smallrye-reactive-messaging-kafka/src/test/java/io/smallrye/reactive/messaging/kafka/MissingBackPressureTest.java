package io.smallrye.reactive.messaging.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;

import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.After;
import org.junit.Test;

import io.reactivex.Flowable;
import io.smallrye.reactive.messaging.annotations.Emitter;
import io.smallrye.reactive.messaging.annotations.Stream;

public class MissingBackPressureTest extends KafkaTestBase {

    private WeldContainer container;

    @After
    public void stop() {
        if (container != null) {
            container.shutdown();
        }
    }

    @Test
    public void testWithInterval() throws InterruptedException {
        KafkaUsage usage = new KafkaUsage();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger expected = new AtomicInteger(0);
        usage.consumeStrings("output", 10, 10, TimeUnit.SECONDS,
                latch::countDown,
                (k, v) -> expected.getAndIncrement());

        Weld weld = baseWeld();
        weld.addBeanClass(MyConfig.class);
        weld.addBeanClass(MyOutgoingBean.class);
        container = weld.initialize();

        assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
        assertThat(expected).hasValueGreaterThanOrEqualTo(10);
    }

    @Test
    public void testWithEmitter() throws InterruptedException {
        List<Map.Entry<String, String>> received = new CopyOnWriteArrayList<>();
        KafkaUsage usage = new KafkaUsage();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger expected = new AtomicInteger(0);
        usage.consumeStrings("output", 10, 10, TimeUnit.SECONDS,
                latch::countDown,
                (k, v) -> {
                    received.add(entry(k, v));
                    expected.getAndIncrement();
                });

        Weld weld = baseWeld();
        weld.addBeanClass(MyConfig.class);
        weld.addBeanClass(MyEmitterBean.class);
        container = weld.initialize();

        MyEmitterBean bean = container.select(MyEmitterBean.class).get();
        bean.run();

        assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();

        bean.stop();
        assertThat(expected).hasValueGreaterThanOrEqualTo(10);

        // Check that the 10 first value matches the emitted values.
        List<KafkaMessage<String, String>> messages = bean.emitted();
        Iterator<Map.Entry<String, String>> iterator = received.iterator();
        for (int i = 0; i < 10; i++) {
            KafkaMessage<String, String> message = messages.get(i);
            Map.Entry<String, String> entry = iterator.next();
            assertThat(entry.getKey()).isEqualTo(message.getKey());
            assertThat(entry.getValue()).isEqualTo(message.getPayload());
        }
    }

    @ApplicationScoped
    public static class MyOutgoingBean {

        private Random random = new Random();

        @Outgoing("temperature-values")
        public Flowable<KafkaMessage<String, String>> generate() {

            return Flowable.interval(100, TimeUnit.MILLISECONDS)
                    .onBackpressureDrop()
                    .map(tick -> {
                        double temperature = new BigDecimal(random.nextGaussian() * 15)
                                .setScale(1, RoundingMode.HALF_UP)
                                .doubleValue();
                        return KafkaMessage.of("1", Instant.now().toEpochMilli() + ";" + temperature);
                    });
        }
    }

    @ApplicationScoped
    public static class MyEmitterBean {

        @Inject
        @Stream("temperature-values")
        private Emitter<KafkaMessage<String, String>> emitter;

        private volatile boolean stop = false;

        private Random random = new Random();
        private List<KafkaMessage<String, String>> emitted = new CopyOnWriteArrayList<>();

        public void stop() {
            this.stop = true;
        }

        public List<KafkaMessage<String, String>> emitted() {
            return emitted;
        }

        public void run() {
            new Thread(() -> {
                while (!stop) {
                    double temperature = new BigDecimal(random.nextGaussian() * 15)
                            .setScale(1, RoundingMode.HALF_UP)
                            .doubleValue();
                    KafkaMessage<String, String> message = KafkaMessage.of("1",
                            Instant.now().toEpochMilli() + ";" + temperature);
                    emitter.send(message);
                    emitted.add(message);
                    nap();
                }
            }).start();
        }

        private void nap() {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @ApplicationScoped
    public static class MyConfig {
        @Produces
        public Config myKafkaSinkConfig() {
            String prefix = "mp.messaging.outgoing.temperature-values.";
            Map<String, Object> config = new HashMap<>();
            config.put(prefix + "connector", KafkaConnector.CONNECTOR_NAME);
            config.put(prefix + "bootstrap.servers", "localhost:9092");
            config.put(prefix + "key.serializer", StringSerializer.class.getName());
            config.put(prefix + "value.serializer", StringSerializer.class.getName());
            config.put(prefix + "acks", "1");
            config.put(prefix + "topic", "output");

            return new MapBasedConfig(config);
        }
    }

}
