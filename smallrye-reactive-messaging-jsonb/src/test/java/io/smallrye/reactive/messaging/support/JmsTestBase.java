package io.smallrye.reactive.messaging.support;

import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.config.inject.ConfigExtension;
import io.smallrye.reactive.messaging.jms.JmsConnector;
import io.smallrye.reactive.messaging.json.jsonb.JsonBMapping;
import io.smallrye.reactive.messaging.json.jsonb.JsonBProvider;
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
import io.smallrye.reactive.messaging.providers.wiring.Wiring;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

/**
 * FIXME?: make the original class reusable?
 */
public class JmsTestBase {

    private static final ArtemisHolder holder = new ArtemisHolder();
    private Weld weld;

    @BeforeEach
    public void startArtemis() {
        holder.start();
    }

    @AfterEach
    public void stopArtemis() {
        holder.stop();
    }

    @BeforeEach
    public void initializeWeld() {
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
        if (withConnectionFactory()) {
            initWeldWithConnectionFactory();
        } else {
            initWithoutConnectionFactory();
        }
    }

    protected boolean withConnectionFactory() {
        return true;
    }

    @AfterEach
    public void cleanUp() {
        weld.shutdown();
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
    }

    protected WeldContainer deploy(Class<?>... beanClass) {
        weld.addBeanClasses(beanClass);
        return weld.initialize();
    }

    private void initWeldWithConnectionFactory() {
        initWithoutConnectionFactory();
        this.weld.addBeanClass(ConnectionFactoryBean.class);
    }

    protected Weld initWithoutConnectionFactory() {
        weld = new Weld();

        // SmallRye config
        ConfigExtension extension = new ConfigExtension();
        weld.addExtension(extension);

        weld.addBeanClass(MediatorFactory.class);
        weld.addBeanClass(MediatorManager.class);
        weld.addBeanClass(InternalChannelRegistry.class);
        weld.addBeanClass(ConfiguredChannelFactory.class);
        weld.addBeanClass(ConnectorFactories.class);
        weld.addBeanClass(ChannelProducer.class);
        weld.addBeanClass(ExecutionHolder.class);
        weld.addBeanClass(WorkerPoolRegistry.class);
        weld.addBeanClass(HealthCenter.class);
        weld.addBeanClass(Wiring.class);
        weld.addExtension(new ReactiveMessagingExtension());
        weld.addBeanClass(EmitterFactoryImpl.class);
        weld.addBeanClass(MutinyEmitterFactoryImpl.class);
        weld.addBeanClass(LegacyEmitterFactoryImpl.class);

        weld.addBeanClass(JmsConnector.class);
        weld.addBeanClass(JsonBProvider.class);
        weld.addBeanClass(JsonBMapping.class);
        weld.disableDiscovery();
        return weld;
    }

    protected void addConfig(MapBasedConfig config) {
        if (config != null) {
            config.write();
        } else {
            MapBasedConfig.cleanup();
        }
    }

}
