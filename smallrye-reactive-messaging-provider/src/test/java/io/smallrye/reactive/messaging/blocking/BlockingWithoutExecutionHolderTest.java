package io.smallrye.reactive.messaging.blocking;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.se.SeContainerInitializer;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.jboss.logmanager.Level;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import io.smallrye.reactive.messaging.annotations.Blocking;
import io.smallrye.reactive.messaging.providers.MediatorFactory;
import io.smallrye.reactive.messaging.providers.connectors.MyDummyConnector;
import io.smallrye.reactive.messaging.providers.connectors.WorkerPoolRegistry;
import io.smallrye.reactive.messaging.providers.extension.ChannelProducer;
import io.smallrye.reactive.messaging.providers.extension.HealthCenter;
import io.smallrye.reactive.messaging.providers.extension.MediatorManager;
import io.smallrye.reactive.messaging.providers.extension.ReactiveMessagingExtension;
import io.smallrye.reactive.messaging.providers.impl.ConfiguredChannelFactory;
import io.smallrye.reactive.messaging.providers.impl.ConnectorFactories;
import io.smallrye.reactive.messaging.providers.impl.InternalChannelRegistry;
import io.smallrye.reactive.messaging.providers.metrics.MetricDecorator;
import io.smallrye.reactive.messaging.providers.metrics.MicrometerDecorator;
import io.smallrye.reactive.messaging.providers.wiring.Wiring;
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
                ConnectorFactories.class,
                ConfiguredChannelFactory.class,
                MicrometerDecorator.class,
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

        assertThatThrownBy(this::initialize).hasStackTraceContaining("@Blocking disabled");

        assertThat(logCapture.records()).isNotNull()
                .filteredOn(r -> r.getMessage().contains("SRMSG00200"))
                .hasSize(1)
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
