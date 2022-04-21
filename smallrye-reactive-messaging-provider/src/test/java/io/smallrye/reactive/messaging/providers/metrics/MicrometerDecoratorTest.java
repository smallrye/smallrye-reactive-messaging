package io.smallrye.reactive.messaging.providers.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

import jakarta.enterprise.inject.Instance;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.smallrye.reactive.messaging.MyCollector;
import io.smallrye.reactive.messaging.WeldTestBase;
import io.smallrye.reactive.messaging.providers.PublisherDecorator;

public class MicrometerDecoratorTest extends WeldTestBase {

    @Test
    void testMicrometerDecoratorAvailable() {
        initialize();
        Instance<PublisherDecorator> decorators = container.select(PublisherDecorator.class);
        assertThat(decorators)
                .anySatisfy(decorator -> assertThat(decorator).isInstanceOfAny(MetricDecorator.class))
                .anySatisfy(decorator -> assertThat(decorator).isInstanceOfAny(MicrometerDecorator.class));
    }

    @BeforeEach
    void initMetricsRegistry() {
        Metrics.globalRegistry.getRegistries().forEach(Metrics.globalRegistry::remove);
        Metrics.addRegistry(new SimpleMeterRegistry());
    }

    @Test
    public void testMetrics() {
        releaseConfig();
        addBeanClass(MetricsTestBean.class);
        initialize();

        MyCollector collector = container.select(MyCollector.class).get();

        await().until(collector::hasCompleted);

        assertEquals(MetricsTestBean.TEST_MESSAGES.size(), getCounter("source").count());

        // Between source and sink, each message is duplicated so we expect double the count for sink
        assertEquals(MetricsTestBean.TEST_MESSAGES.size() * 2, getCounter("sink").count());
    }

    private Counter getCounter(String channelName) {
        return Metrics.counter("mp.messaging.message.count", "channel", channelName);
    }

    @Test
    void testMicrometerDisabled() {
        installConfig("src/test/resources/config/micrometer-disabled.properties");
        addBeanClass(MetricsTestBean.class);
        initialize();

        MyCollector collector = container.select(MyCollector.class).get();
        await().until(collector::hasCompleted);

        assertEquals(0, getCounter("source").count());
    }
}
