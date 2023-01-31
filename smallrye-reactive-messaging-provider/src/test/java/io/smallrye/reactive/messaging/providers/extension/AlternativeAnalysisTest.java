package io.smallrye.reactive.messaging.providers.extension;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.se.SeContainer;
import jakarta.enterprise.inject.se.SeContainerInitializer;
import jakarta.enterprise.inject.spi.Bean;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.Invoker;
import io.smallrye.reactive.messaging.MediatorConfiguration;
import io.smallrye.reactive.messaging.Shape;
import io.smallrye.reactive.messaging.providers.DefaultMediatorConfiguration;
import io.smallrye.reactive.messaging.providers.MediatorFactory;
import io.smallrye.reactive.messaging.providers.connectors.ExecutionHolder;
import io.smallrye.reactive.messaging.providers.connectors.MyDummyConnector;
import io.smallrye.reactive.messaging.providers.connectors.WorkerPoolRegistry;
import io.smallrye.reactive.messaging.providers.impl.ConfiguredChannelFactory;
import io.smallrye.reactive.messaging.providers.impl.ConnectorFactories;
import io.smallrye.reactive.messaging.providers.impl.InternalChannelRegistry;
import io.smallrye.reactive.messaging.providers.locals.ContextDecorator;
import io.smallrye.reactive.messaging.providers.wiring.Wiring;

/**
 * Test the analysis as used in Quarkus.
 */
public class AlternativeAnalysisTest {

    protected SeContainerInitializer initializer;
    protected SeContainer container;

    @AfterEach
    void cleanup() {
        if (container != null) {
            container.close();
        }
    }

    @Test
    void testAlternativeAnalysis() {
        initializer = SeContainerInitializer.newInstance();
        initializer.disableDiscovery();
        initializer.addBeanClasses(MediatorFactory.class,
                Wiring.class,
                ExecutionHolder.class,
                MediatorManager.class,
                WorkerPoolRegistry.class,
                InternalChannelRegistry.class,
                ChannelProducer.class,
                ConfiguredChannelFactory.class,
                ConnectorFactories.class,
                HealthCenter.class,
                ContextDecorator.class,
                // Messaging provider
                MyDummyConnector.class,

                // SmallRye config
                io.smallrye.config.inject.ConfigProducer.class);
        initializer.addBeanClasses(MyApplicationWithIncomingAndOutgoing.class, Empty.class);

        container = initializer.initialize();
        MediatorManager manager = container.select(MediatorManager.class).get();

        Set<Bean<?>> beans = container.getBeanManager().getBeans(Empty.class);
        manager.analyze(Empty.class, beans.iterator().next());
        assertThat(manager.getCollected().mediators()).isEmpty();

        beans = container.getBeanManager().getBeans(MyApplicationWithIncomingAndOutgoing.class);
        manager.analyze(MyApplicationWithIncomingAndOutgoing.class, beans.iterator().next());
        assertThat(manager.getCollected().mediators()).hasSize(3);

        for (MediatorConfiguration c : manager.getCollected().mediators()) {
            DefaultMediatorConfiguration configuration = new DefaultMediatorConfiguration(c.getMethod(), c.getBean()) {
                @Override
                public Class<? extends Invoker> getInvokerClass() {
                    return MyInvoker.class;
                }

                @Override
                public Shape shape() {
                    return c.shape();
                }

                @Override
                public Production production() {
                    return c.production();
                }

                @Override
                public Consumption consumption() {
                    return c.consumption();
                }
            };
            manager.createMediator(configuration);
        }

        manager.start();

        MyApplicationWithIncomingAndOutgoing instance = container.select(MyApplicationWithIncomingAndOutgoing.class).get();
        assertThat(instance.getList()).hasSize(10);

    }

    /**
     * A very dumb invoker.
     */
    public static class MyInvoker implements Invoker {

        private final MyApplicationWithIncomingAndOutgoing instance;

        public MyInvoker(Object obj) {
            this.instance = (MyApplicationWithIncomingAndOutgoing) obj;
        }

        @Override
        public Object invoke(Object... args) {
            if (args.length == 0) {
                //noinspection ReactiveStreamsUnusedPublisher
                return instance.stream();
            }
            if (args.length == 1 && args[0] instanceof Integer) {
                return instance.process((Integer) args[0]);
            }
            if (args.length == 1 && args[0] instanceof String) {
                instance.consume((String) args[0]);
                return null;
            }
            throw new IllegalStateException("Unexpected invocation");
        }
    }

    @ApplicationScoped
    static class Empty {

        @SuppressWarnings("unused")
        public void doNothing() {

        }

    }

    @ApplicationScoped
    static class MyApplicationWithIncomingAndOutgoing {

        private final List<String> list = new CopyOnWriteArrayList<>();

        @Outgoing("a")
        Multi<Integer> stream() {
            return Multi.createFrom().range(0, 10);
        }

        @Incoming("a")
        @Outgoing("b")
        String process(Integer i) {
            return Integer.toString(i);
        }

        @Incoming("b")
        void consume(String s) {
            list.add(s);
        }

        public List<String> getList() {
            return list;
        }
    }

}
