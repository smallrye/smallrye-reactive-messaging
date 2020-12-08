package io.smallrye.reactive.messaging.jms.support;

import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.After;
import org.junit.Before;

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
import io.smallrye.reactive.messaging.jms.JmsConnector;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class JmsTestBase {

    private static final ArtemisHolder holder = new ArtemisHolder();
    private Weld weld;

    @Before
    public void startArtemis() {
        holder.start();
    }

    @After
    public void stopArtemis() {
        holder.stop();
    }

    @Before
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

    @After
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
        weld.addBeanClass(ChannelProducer.class);
        weld.addBeanClass(ExecutionHolder.class);
        weld.addBeanClass(WorkerPoolRegistry.class);
        weld.addBeanClass(HealthCenter.class);
        weld.addExtension(new ReactiveMessagingExtension());
        weld.addBeanClass(JmsConnector.class);
        weld.disableDiscovery();
        return weld;
    }

    protected void addConfig(MapBasedConfig config) {
        if (config != null) {
            config.write();
        } else {
            MapBasedConfig.clear();
        }
    }

}
