package io.smallrye.reactive.messaging.aws.sqs;

import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.spi.BeanManager;

import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorLiteral;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.config.inject.ConfigExtension;
import io.smallrye.reactive.messaging.providers.MediatorFactory;
import io.smallrye.reactive.messaging.providers.connectors.ExecutionHolder;
import io.smallrye.reactive.messaging.providers.connectors.WorkerPoolRegistry;
import io.smallrye.reactive.messaging.providers.extension.ChannelProducer;
import io.smallrye.reactive.messaging.providers.extension.EmitterFactoryImpl;
import io.smallrye.reactive.messaging.providers.extension.HealthCenter;
import io.smallrye.reactive.messaging.providers.extension.LegacyEmitterFactoryImpl;
import io.smallrye.reactive.messaging.providers.extension.MediatorManager;
import io.smallrye.reactive.messaging.providers.extension.MutinyEmitterFactoryImpl;
import io.smallrye.reactive.messaging.providers.extension.ReactiveMessagingExtension;
import io.smallrye.reactive.messaging.providers.impl.ConfiguredChannelFactory;
import io.smallrye.reactive.messaging.providers.impl.ConnectorFactories;
import io.smallrye.reactive.messaging.providers.impl.InternalChannelRegistry;
import io.smallrye.reactive.messaging.providers.metrics.MetricDecorator;
import io.smallrye.reactive.messaging.providers.metrics.MicrometerDecorator;
import io.smallrye.reactive.messaging.providers.wiring.Wiring;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class BasicTest {

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
        weld.addBeanClass(ConnectorFactories.class);
        weld.addBeanClass(ConfiguredChannelFactory.class);
        weld.addBeanClass(ChannelProducer.class);
        weld.addBeanClass(ExecutionHolder.class);
        weld.addBeanClass(WorkerPoolRegistry.class);
        weld.addBeanClass(HealthCenter.class);
        weld.addBeanClass(Wiring.class);
        weld.addExtension(new ReactiveMessagingExtension());

        weld.addBeanClass(EmitterFactoryImpl.class);
        weld.addBeanClass(MutinyEmitterFactoryImpl.class);
        weld.addBeanClass(LegacyEmitterFactoryImpl.class);

        weld.addBeanClass(SqsConnector.class);
        weld.addBeanClass(MetricDecorator.class);
        weld.addBeanClass(MicrometerDecorator.class);
        weld.disableDiscovery();
    }

    public BeanManager getBeanManager() {
        if (container == null) {
            runApplication(new MapBasedConfig());
        }
        return container.getBeanManager();
    }

    public <T> T get(Class<T> clazz) {
        return getBeanManager().createInstance().select(clazz).get();
    }

    public void runApplication(MapBasedConfig config) {
        if (config != null) {
            config.write();
        } else {
            MapBasedConfig.cleanup();
        }

        container = weld.initialize();
    }

    public <T> T runApplication(MapBasedConfig config, Class<T> clazz) {
        weld.addBeanClass(clazz);
        runApplication(config);
        return get(clazz);
    }

    @AfterEach
    public void stopContainer() {
        if (container != null) {
            // TODO Explicitly close the connector
            getBeanManager().createInstance()
                    .select(SqsConnector.class, ConnectorLiteral.of(SqsConnector.CONNECTOR_NAME)).get();
            container.close();
        }
        // Release the config objects
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
    }

    @ApplicationScoped
    public static class MyApp {

        List<String> received = new CopyOnWriteArrayList<>();

        @Incoming("data")
        void consume(String msg) {
            received.add(msg);
        }

        public List<String> received() {
            return received;

        }
    }

    @Test
    void testBasic() {
        var queue = "test-queue";
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.data.queue", queue)
                .with("mp.messaging.incoming.data.endpointOverride", "http://localhost:4566")
                .with("mp.messaging.incoming.data.connector", SqsConnector.CONNECTOR_NAME);

        MyApp app = runApplication(config, MyApp.class);

        int expected = 1;
        // produce expected number of messages to myTopic

        // wait until app received
        await().until(() -> app.received().size() == expected);
    }
}
