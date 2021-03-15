package io.smallrye.reactive.messaging.metrics;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

import javax.enterprise.util.AnnotationLiteral;

import org.eclipse.microprofile.metrics.Counter;
import org.eclipse.microprofile.metrics.MetricRegistry;
import org.eclipse.microprofile.metrics.MetricRegistry.Type;
import org.eclipse.microprofile.metrics.Tag;
import org.eclipse.microprofile.metrics.annotation.RegistryType;
import org.junit.jupiter.api.Test;

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

        await().until(collector::hasCompleted);

        assertEquals(MetricsTestBean.TEST_MESSAGES.size(), getCounter("source").getCount());

        // Between source and sink, each message is duplicated so we expect double the count for sink
        assertEquals(MetricsTestBean.TEST_MESSAGES.size() * 2, getCounter("sink").getCount());
    }

    private Counter getCounter(String channelName) {
        MetricRegistry registry = container.select(MetricRegistry.class, RegistryTypeLiteral.BASE).get();
        return registry.counter("mp.messaging.message.count", new Tag("channel", channelName));
    }

    @SuppressWarnings("serial")
    private static class RegistryTypeLiteral extends AnnotationLiteral<RegistryType> implements RegistryType {
        public static final RegistryTypeLiteral BASE = new RegistryTypeLiteral(MetricRegistry.Type.BASE);

        private Type registryType;

        public RegistryTypeLiteral(Type registryType) {
            this.registryType = registryType;
        }

        @Override
        public Type type() {
            return registryType;
        }
    }
}
