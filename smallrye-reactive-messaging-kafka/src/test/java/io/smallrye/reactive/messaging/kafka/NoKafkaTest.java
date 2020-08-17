package io.smallrye.reactive.messaging.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.After;
import org.junit.Test;

import io.reactivex.Flowable;
import io.reactivex.exceptions.MissingBackpressureException;
import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.reactive.messaging.extension.HealthCenter;
import io.smallrye.reactive.messaging.health.HealthReport;

public class NoKafkaTest {

    private Weld container;

    @After
    public void tearDown() {
        if (container != null) {
            container.shutdown();
        }
        KafkaTestBase.stopKafkaBroker();
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
    }

    @Test
    public void testOutgoingWithoutKafkaCluster() throws IOException, InterruptedException {
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

        container = KafkaTestBase.baseWeld();
        KafkaTestBase.addConfig(myKafkaSinkConfigWithoutBlockLimit());
        container.addBeanClasses(MyOutgoingBean.class);
        WeldContainer weld = container.initialize();

        assertThat(expected).hasValue(0);

        await().until(() -> {
            HealthReport readiness = getHealth(weld).getReadiness();
            return !readiness.isOk();
        });

        await().until(() -> {
            // liveness is ok, as we don't check the connection with the broker
            HealthReport liveness = getHealth(weld).getLiveness();
            return liveness.isOk();
        });

        KafkaTestBase.startKafkaBroker();

        await().until(() -> {
            HealthReport readiness = getHealth(weld).getReadiness();
            return readiness.isOk();
        });

        await().until(() -> {
            HealthReport liveness = getHealth(weld).getLiveness();
            return liveness.isOk();
        });

        await().until(() -> received.size() == 3);

    }

    private HealthCenter getHealth(WeldContainer container) {
        return container.getBeanManager().createInstance().select(HealthCenter.class).get();
    }

    @Test
    public void testIncomingWithoutKafkaCluster() throws IOException, InterruptedException {
        KafkaUsage usage = new KafkaUsage();
        container = KafkaTestBase.baseWeld();
        KafkaTestBase.addConfig(myKafkaSourceConfig());
        container.addBeanClasses(MyIncomingBean.class);
        WeldContainer weld = container.initialize();

        MyIncomingBean bean = weld.select(MyIncomingBean.class).get();
        assertThat(bean.received()).hasSize(0);

        await().until(() -> {
            HealthReport readiness = getHealth(weld).getReadiness();
            return !readiness.isOk();
        });

        await().until(() -> {
            // liveness is ok, as we don't check the connection with the broker
            HealthReport liveness = getHealth(weld).getLiveness();
            return liveness.isOk();
        });

        KafkaTestBase.startKafkaBroker();

        await().until(() -> {
            HealthReport readiness = getHealth(weld).getReadiness();
            return readiness.isOk();
        });

        await().until(() -> {
            HealthReport liveness = getHealth(weld).getLiveness();
            return liveness.isOk();
        });

        AtomicInteger counter = new AtomicInteger();
        usage.produceIntegers(5, null, () -> new ProducerRecord<>("output", "1", counter.getAndIncrement()));

        await().until(() -> bean.received().size() == 5);

    }

    @Test
    public void testOutgoingWithoutKafkaClusterWithoutBackPressure() throws InterruptedException {
        container = KafkaTestBase.baseWeld();
        KafkaTestBase.addConfig(myKafkaSinkConfig());
        container.addBeanClasses(MyOutgoingBeanWithoutBackPressure.class);
        WeldContainer weld = this.container.initialize();

        await().until(() -> {
            HealthReport readiness = getHealth(weld).getReadiness();
            return !readiness.isOk();
        });

        await().until(() -> {
            // Failure caught
            HealthReport liveness = getHealth(weld).getLiveness();
            return !liveness.isOk();
        });

        MyOutgoingBeanWithoutBackPressure bean = weld
                .select(MyOutgoingBeanWithoutBackPressure.class).get();
        Throwable throwable = bean.error();
        assertThat(throwable).isNotNull();
        assertThat(throwable).isInstanceOf(MissingBackpressureException.class);
    }

    @ApplicationScoped
    public static class MyOutgoingBean {

        final AtomicInteger counter = new AtomicInteger();

        @Outgoing("temperature-values")
        public Flowable<String> generate() {
            return Flowable.generate(e -> {
                int i = counter.getAndIncrement();
                if (i == 3) {
                    e.onComplete();
                } else {
                    e.onNext(Integer.toString(i));
                }
            });
        }
    }

    @ApplicationScoped
    public static class MyIncomingBean {

        final List<Integer> received = new CopyOnWriteArrayList<>();

        @Incoming("temperature-values")
        public void consume(int p) {
            received.add(p);
        }

        public List<Integer> received() {
            return received;
        }
    }

    @ApplicationScoped
    public static class MyOutgoingBeanWithoutBackPressure {

        private final AtomicReference<Throwable> error = new AtomicReference<>();

        public Throwable error() {
            return error.get();
        }

        @Outgoing("temperature-values")
        public Flowable<String> generate() {
            return Flowable.interval(200, TimeUnit.MILLISECONDS)
                    // No overflow management - we want it to fail.
                    .map(l -> Long.toString(l))
                    .doOnError(error::set);
        }
    }

    private MapBasedConfig myKafkaSourceConfig() {
        String prefix = "mp.messaging.incoming.temperature-values.";
        Map<String, Object> config = new HashMap<>();
        config.put(prefix + "connector", KafkaConnector.CONNECTOR_NAME);
        config.put(prefix + "value.deserializer", IntegerDeserializer.class.getName());
        config.put(prefix + "topic", "output");
        config.put(prefix + "commit-strategy", "latest");

        return new MapBasedConfig(config);
    }

    private MapBasedConfig myKafkaSinkConfig() {
        String prefix = "mp.messaging.outgoing.temperature-values.";
        Map<String, Object> config = new HashMap<>();
        config.put(prefix + "connector", KafkaConnector.CONNECTOR_NAME);
        config.put(prefix + "value.serializer", StringSerializer.class.getName());
        config.put(prefix + "max-inflight-messages", "2");
        config.put(prefix + "max.block.ms", 1000);
        config.put(prefix + "topic", "output");

        return new MapBasedConfig(config);
    }

    private MapBasedConfig myKafkaSinkConfigWithoutBlockLimit() {
        String prefix = "mp.messaging.outgoing.temperature-values.";
        Map<String, Object> config = new HashMap<>();
        config.put(prefix + "connector", KafkaConnector.CONNECTOR_NAME);
        config.put(prefix + "value.serializer", StringSerializer.class.getName());
        config.put(prefix + "topic", "output");

        return new MapBasedConfig(config);
    }

}
