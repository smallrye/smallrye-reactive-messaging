package io.smallrye.reactive.messaging.eventbus;

import java.io.File;

import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.weld.environment.se.Weld;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.reactive.messaging.MediatorFactory;
import io.smallrye.reactive.messaging.providers.connectors.ExecutionHolder;
import io.smallrye.reactive.messaging.providers.connectors.WorkerPoolRegistry;
import io.smallrye.reactive.messaging.providers.extension.HealthCenter;
import io.smallrye.reactive.messaging.providers.extension.MediatorManager;
import io.smallrye.reactive.messaging.providers.extension.ReactiveMessagingExtension;
import io.smallrye.reactive.messaging.providers.impl.ConfiguredChannelFactory;
import io.smallrye.reactive.messaging.providers.impl.InternalChannelRegistry;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.smallrye.reactive.messaging.providers.wiring.Wiring;
import io.vertx.mutiny.core.Vertx;

public class EventbusTestBase {

    protected EventBusUsage usage;
    Vertx vertx;

    static Weld baseWeld() {
        Weld weld = new Weld();
        weld.addBeanClass(MediatorFactory.class);
        weld.addBeanClass(MediatorManager.class);
        weld.addBeanClass(InternalChannelRegistry.class);
        weld.addBeanClass(ConfiguredChannelFactory.class);
        weld.addBeanClass(ExecutionHolder.class);
        weld.addBeanClass(WorkerPoolRegistry.class);
        weld.addBeanClass(HealthCenter.class);
        weld.addBeanClass(Wiring.class);
        weld.addExtension(new ReactiveMessagingExtension());
        weld.addBeanClass(VertxEventBusConnector.class);
        weld.addBeanClass(EmitterFactoryImpl.class);
        weld.addBeanClass(MutinyEmitterFactoryImpl.class);
        weld.addBeanClass(LegacyEmitterFactoryImpl.class);
        weld.addExtension(new io.smallrye.config.inject.ConfigExtension());

        weld.disableDiscovery();
        return weld;
    }

    public static void addConfig(MapBasedConfig config) {
        if (config != null) {
            config.write();
        } else {
            clear();
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public static void clear() {
        File out = new File("target/test-classes/META-INF/microprofile-config.properties");
        if (out.isFile()) {
            out.delete();
        }
    }

    @BeforeEach
    public void setup() {
        vertx = Vertx.vertx();
        usage = new EventBusUsage(vertx.eventBus().getDelegate());
    }

    @AfterEach
    public void tearDown() {
        vertx.closeAndAwait();
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
    }

}
