package io.smallrye.reactive.messaging.jms.support;

import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.config.inject.ConfigExtension;
import io.smallrye.reactive.messaging.MediatorFactory;
import io.smallrye.reactive.messaging.extension.ChannelProducer;
import io.smallrye.reactive.messaging.extension.MediatorManager;
import io.smallrye.reactive.messaging.extension.ReactiveMessagingExtension;
import io.smallrye.reactive.messaging.impl.ConfiguredChannelFactory;
import io.smallrye.reactive.messaging.impl.InternalChannelRegistry;
import io.smallrye.reactive.messaging.jms.JmsConnector;

public class JmsTestBase {

    private static ArtemisHolder holder = new ArtemisHolder();
    private Weld weld;
    private WeldContainer container;

    @BeforeClass
    public static void startArtemis() {
        holder.start();
    }

    @AfterClass
    public static void stopArtemis() {
        holder.stop();
    }

    @Before
    public void initializeWeld() {
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
        baseWeld();
    }

    @After
    public void cleanUp() {
        weld.shutdown();
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
    }

    public WeldContainer deploy(Class<?>... beanClass) {
        weld.addBeanClasses(beanClass);
        container = weld.initialize();
        return container;
    }

    private Weld baseWeld() {
        weld = new Weld();

        // SmallRye config
        ConfigExtension extension = new ConfigExtension();
        weld.addExtension(extension);

        weld.addBeanClass(MediatorFactory.class);
        weld.addBeanClass(MediatorManager.class);
        weld.addBeanClass(InternalChannelRegistry.class);
        weld.addBeanClass(ConfiguredChannelFactory.class);
        weld.addBeanClass(ChannelProducer.class);
        weld.addExtension(new ReactiveMessagingExtension());
        weld.addBeanClass(JmsConnector.class);
        weld.addBeanClass(ConnectionFactoryBean.class);
        weld.disableDiscovery();
        return weld;
    }

    public void addConfig(MapBasedConfig config) {
        if (config != null) {
            config.write();
        } else {
            MapBasedConfig.clear();
        }
    }

}
