package io.smallrye.reactive.messaging.blocking;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.se.SeContainerInitializer;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.jboss.logmanager.Level;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.MediatorFactory;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import io.smallrye.reactive.messaging.annotations.Blocking;
import io.smallrye.reactive.messaging.connectors.MyDummyConnector;
import io.smallrye.reactive.messaging.connectors.WorkerPoolRegistry;
import io.smallrye.reactive.messaging.extension.ChannelProducer;
import io.smallrye.reactive.messaging.extension.HealthCenter;
import io.smallrye.reactive.messaging.extension.MediatorManager;
import io.smallrye.reactive.messaging.extension.ReactiveMessagingExtension;
import io.smallrye.reactive.messaging.impl.ConfiguredChannelFactory;
import io.smallrye.reactive.messaging.impl.InternalChannelRegistry;
import io.smallrye.reactive.messaging.impl.LegacyConfiguredChannelFactory;
import io.smallrye.reactive.messaging.metrics.MetricDecorator;
import io.smallrye.reactive.messaging.wiring.Wiring;
import io.smallrye.testing.logging.LogCapture;

public class BlockingWithoutExecutionHolderTest extends WeldTestBaseWithoutTails {

    @RegisterExtension
    static LogCapture logCapture = LogCapture.with(r -> "io.smallrye.reactive.messaging.provider".equals(r.getLoggerName()),
            Level.ERROR);

    @BeforeEach
    public void setUp() {
        initializer = SeContainerInitializer.newInstance();

        initializer.addBeanClasses(MediatorFactory.class,
                Wiring.class,
                // No holder
                MediatorManager.class,
                WorkerPoolRegistry.class,
                InternalChannelRegistry.class,
                ChannelProducer.class,
                ConfiguredChannelFactory.class,
                LegacyConfiguredChannelFactory.class,
                MetricDecorator.class,
                HealthCenter.class,
                // Messaging provider
                MyDummyConnector.class,

                // SmallRye config
                io.smallrye.config.inject.ConfigProducer.class);

        List<Class<?>> beans = getBeans();
        initializer.addBeanClasses(beans.toArray(new Class<?>[0]));
        initializer.disableDiscovery();
        initializer.addExtensions(new ReactiveMessagingExtension());
    }

    @Test
    public void testBlockingWithoutExecutionHolder() {
        addBeanClass(MyApplication.class);
        initialize();

        assertThat(logCapture.records()).isNotNull()
                .filteredOn(r -> r.getMessage().contains("SRMSG00200"))
                .hasSize(3)
                .allSatisfy(r -> assertThat(r.getMessage())
                        .contains(
                                "io.smallrye.reactive.messaging.blocking.BlockingWithoutExecutionHolderTest$MyApplication#consume has thrown an exception"));
    }

    @ApplicationScoped
    static class MyApplication {

        @Outgoing("a")
        public Multi<Integer> produce() {
            return Multi.createFrom().items(1, 2, 3);
        }

        @Incoming("a")
        @Blocking
        public void consume(int i) {

        }

    }
}
