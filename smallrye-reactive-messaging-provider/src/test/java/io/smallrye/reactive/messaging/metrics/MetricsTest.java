package io.smallrye.reactive.messaging.metrics;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;

import org.eclipse.microprofile.metrics.Counter;
import org.eclipse.microprofile.metrics.MetricRegistry;
import org.eclipse.microprofile.metrics.Tag;
import org.junit.Test;

import io.smallrye.metrics.setup.MetricCdiInjectionExtension;
import io.smallrye.reactive.messaging.MyCollector;
import io.smallrye.reactive.messaging.WeldTestBase;

public class MetricsTest extends WeldTestBase {

    @Test
    public void testMetrics() {
        addBeanClass(MetricsTestBean.class);
        addExtensionClass(MetricCdiInjectionExtension.class);
        initialize();

        MyCollector collector = container.select(MyCollector.class).get();

        await().until(() -> collector.hasCompleted());

        assertEquals(MetricsTestBean.TEST_MESSAGES.size(), getCounter("source").getCount());

        // Between source and sink, each message is duplicated so we expect double the count for sink
        assertEquals(MetricsTestBean.TEST_MESSAGES.size() * 2, getCounter("sink").getCount());
    }

    private Counter getCounter(String channelName) {
        MetricRegistry registry = container.select(MetricRegistry.class).get();
        return registry.counter("mp.messaging.message.count", new Tag("channel", channelName));
    }
}
