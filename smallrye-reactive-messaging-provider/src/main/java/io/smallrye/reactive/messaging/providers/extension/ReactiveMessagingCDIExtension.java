package io.smallrye.reactive.messaging.providers.extension;

import io.smallrye.reactive.messaging.providers.MediatorFactory;
import io.smallrye.reactive.messaging.providers.connectors.ExecutionHolder;
import io.smallrye.reactive.messaging.providers.connectors.WorkerPoolRegistry;
import io.smallrye.reactive.messaging.providers.impl.ConfiguredChannelFactory;
import io.smallrye.reactive.messaging.providers.impl.ConnectorFactories;
import io.smallrye.reactive.messaging.providers.impl.InternalChannelRegistry;
import io.smallrye.reactive.messaging.providers.locals.ContextDecorator;
import io.smallrye.reactive.messaging.providers.wiring.Wiring;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.spi.BeanManager;
import jakarta.enterprise.inject.spi.BeforeBeanDiscovery;
import jakarta.enterprise.inject.spi.Extension;
import java.util.Set;

public class ReactiveMessagingCDIExtension implements Extension {

    private static final Set<Class<?>> DEFAULT_BEAN_CLASSES = Set.of(
        MediatorFactory.class,
        MediatorManager.class,
        InternalChannelRegistry.class,
        ConnectorFactories.class,
        ConfiguredChannelFactory.class,
        ChannelProducer.class,
        ExecutionHolder.class,
        WorkerPoolRegistry.class,
        HealthCenter.class,
        Wiring.class,
        EmitterFactoryImpl.class,
        MutinyEmitterFactoryImpl.class,
        LegacyEmitterFactoryImpl.class,
        ContextDecorator.class
    );

    void addDefaultBeans(@Observes BeforeBeanDiscovery bbd, BeanManager bm) {
        for (Class<?> clazz : DEFAULT_BEAN_CLASSES) {
            bbd.addAnnotatedType(bm.createAnnotatedType(clazz), clazz.getName());
        }
    }
}
