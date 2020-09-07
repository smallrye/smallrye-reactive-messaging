package io.smallrye.reactive.messaging.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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
import io.smallrye.mutiny.Multi;
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
        KafkaTestBase.stopKafkaBroker();

        String topic = UUID.randomUUID().toString();
        List<Map.Entry<String, String>> received = new CopyOnWriteArrayList<>();
        KafkaUsage usage = new KafkaUsage();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger expected = new AtomicInteger(0);

        container = KafkaTestBase.baseWeld();
        KafkaTestBase.addConfig(myKafkaSinkConfigWithoutBlockLimit(topic));
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

        usage.consumeStrings(topic, 3, 10, TimeUnit.MINUTES,
                latch::countDown,
                (k, v) -> {
                    received.add(entry(k, v));
                    expected.getAndIncrement();
                });

        await().until(() -> received.size() == 3);

        latch.await();
    }

    private HealthCenter getHealth(WeldContainer container) {
        return container.getBeanManager().createInstance().select(HealthCenter.class).get();
    }

    @Test
    public void testIncomingWithoutKafkaCluster() throws IOException {
        KafkaTestBase.stopKafkaBroker();

        String topic = UUID.randomUUID().toString();
        KafkaUsage usage = new KafkaUsage();
        container = KafkaTestBase.baseWeld();
        KafkaTestBase.addConfig(myKafkaSourceConfig(topic));
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
        usage.produceIntegers(5, null, () -> new ProducerRecord<>(topic, "1", counter.getAndIncrement()));

        await().until(() -> bean.received().size() == 5);

    }

    @Test
    public void testOutgoingWithoutKafkaClusterWithoutBackPressure() {
        String topic = UUID.randomUUID().toString();
        container = KafkaTestBase.baseWeld();
        KafkaTestBase.addConfig(myKafkaSinkConfig(topic));
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
        public Multi<String> generate() {
            return Multi.createFrom().emitter(e -> {
                e.emit("0");
                e.emit("1");
                e.emit("2");
                e.complete();
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

    private MapBasedConfig myKafkaSourceConfig(String topic) {
        MapBasedConfig.ConfigBuilder builder = new MapBasedConfig.ConfigBuilder("mp.messaging.incoming.temperature-values");
        builder.put("connector", KafkaConnector.CONNECTOR_NAME);
        builder.put("value.deserializer", IntegerDeserializer.class.getName());
        builder.put("topic", topic);
        builder.put("commit-strategy", "latest");

        return new MapBasedConfig(builder.build());
    }

    private MapBasedConfig myKafkaSinkConfig(String topic) {
        MapBasedConfig.ConfigBuilder builder = new MapBasedConfig.ConfigBuilder("mp.messaging.outgoing.temperature-values");
        builder.put("connector", KafkaConnector.CONNECTOR_NAME);
        builder.put("value.serializer", StringSerializer.class.getName());
        builder.put("max-inflight-messages", "2");
        builder.put("max.block.ms", 1000);
        builder.put("topic", topic);

        return new MapBasedConfig(builder.build());
    }

    private MapBasedConfig myKafkaSinkConfigWithoutBlockLimit(String topic) {
        MapBasedConfig.ConfigBuilder builder = new MapBasedConfig.ConfigBuilder("mp.messaging.outgoing.temperature-values");
        builder.put("connector", KafkaConnector.CONNECTOR_NAME);
        builder.put("value.serializer", StringSerializer.class.getName());
        builder.put("topic", topic);

        return new MapBasedConfig(builder.build());
    }

}
