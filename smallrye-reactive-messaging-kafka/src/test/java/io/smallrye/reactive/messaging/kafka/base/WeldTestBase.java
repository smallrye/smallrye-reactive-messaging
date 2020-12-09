package io.smallrye.reactive.messaging.kafka.base;

import java.util.HashMap;

import javax.enterprise.inject.spi.BeanManager;

import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorLiteral;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.config.inject.ConfigExtension;
import io.smallrye.reactive.messaging.MediatorFactory;
import io.smallrye.reactive.messaging.connectors.ExecutionHolder;
import io.smallrye.reactive.messaging.connectors.WorkerPoolRegistry;
import io.smallrye.reactive.messaging.extension.ChannelProducer;
import io.smallrye.reactive.messaging.extension.HealthCenter;
import io.smallrye.reactive.messaging.extension.MediatorManager;
import io.smallrye.reactive.messaging.extension.ReactiveMessagingExtension;
import io.smallrye.reactive.messaging.impl.ConfiguredChannelFactory;
import io.smallrye.reactive.messaging.impl.InternalChannelRegistry;
import io.smallrye.reactive.messaging.kafka.KafkaCDIEvents;
import io.smallrye.reactive.messaging.kafka.KafkaConnector;
import io.smallrye.reactive.messaging.kafka.commit.KafkaThrottledLatestProcessedCommit;
import io.smallrye.reactive.messaging.wiring.Wiring;

public class WeldTestBase {

    protected Weld weld;
    protected WeldContainer container;

    @BeforeEach
    public void initWeld() {
        weld = new Weld();

        // SmallRye config
        ConfigExtension extension = new ConfigExtension();
        weld.addExtension(extension);

        weld.addBeanClass(MediatorFactory.class);
        weld.addBeanClass(MediatorManager.class);
        weld.addBeanClass(InternalChannelRegistry.class);
        weld.addBeanClass(ConfiguredChannelFactory.class);
        weld.addBeanClass(ChannelProducer.class);
        weld.addBeanClass(ExecutionHolder.class);
        weld.addBeanClass(WorkerPoolRegistry.class);
        weld.addBeanClass(HealthCenter.class);
        weld.addBeanClass(Wiring.class);
        weld.addExtension(new ReactiveMessagingExtension());

        weld.addBeanClass(KafkaCDIEvents.class);
        weld.addBeanClass(KafkaConnector.class);
        weld.disableDiscovery();
    }

    @AfterEach
    public void stopContainer() {
        if (container != null) {
            // Explicitly close the connector
            getBeanManager().createInstance()
                    .select(KafkaConnector.class, ConnectorLiteral.of("smallrye-kafka")).get().terminate(new Object());
            container.close();
        }
        // Release the config objects
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
        KafkaThrottledLatestProcessedCommit.clearCache();
    }

    public BeanManager getBeanManager() {
        if (container == null) {
            runApplication(new MapBasedConfig(new HashMap<>()));
        }
        return container.getBeanManager();
    }

    public void addBeans(Class<?>... clazzes) {
        weld.addBeanClasses(clazzes);
    }

    public <T> T get(Class<T> clazz) {
        return getBeanManager().createInstance().select(clazz).get();
    }

    public <T> T runApplication(MapBasedConfig config, Class<T> clazz) {
        weld.addBeanClass(clazz);
        runApplication(config);
        return get(clazz);
    }

    public void runApplication(MapBasedConfig config) {
        if (config != null) {
            config.write();
        } else {
            MapBasedConfig.cleanup();
        }

        container = weld.initialize();
    }

    public static void addConfig(MapBasedConfig config) {
        if (config != null) {
            config.write();
        } else {
            MapBasedConfig.cleanup();
        }
    }

    public HealthCenter getHealth() {
        if (container == null) {
            throw new IllegalStateException("Application not started");
        }
        return container.getBeanManager().createInstance().select(HealthCenter.class).get();
    }

    public boolean isReady() {
        getHealth().getReadiness().getChannels().forEach(ci -> System.out.println(ci.getChannel() + " / " + ci.getMessage()));
        return getHealth().getReadiness().isOk();
    }

    public boolean isAlive() {
        return getHealth().getLiveness().isOk();
    }
}
