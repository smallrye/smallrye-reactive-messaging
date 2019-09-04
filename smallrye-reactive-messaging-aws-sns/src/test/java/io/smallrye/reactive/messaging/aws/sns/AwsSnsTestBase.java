package io.smallrye.reactive.messaging.aws.sns;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.jboss.weld.environment.se.Weld;
import org.junit.After;
import org.junit.Before;

import io.smallrye.config.inject.ConfigExtension;
import io.smallrye.reactive.messaging.MediatorFactory;
import io.smallrye.reactive.messaging.extension.MediatorManager;
import io.smallrye.reactive.messaging.extension.ReactiveMessagingExtension;
import io.smallrye.reactive.messaging.extension.StreamProducer;
import io.smallrye.reactive.messaging.impl.ConfiguredChannelFactory;
import io.smallrye.reactive.messaging.impl.InternalChannelRegistry;
import io.vertx.reactivex.core.Vertx;

public class AwsSnsTestBase {

    Vertx vertx;

    @Before
    public void setup() {
        vertx = Vertx.vertx();
    }

    @After
    public void tearDown() {
        vertx.close();
    }

    static Weld baseWeld() {
        Weld weld = new Weld();
        // SmallRye config
        ConfigExtension extension = new ConfigExtension();
        weld.addExtension(extension);

        weld.addBeanClass(MediatorFactory.class);
        weld.addBeanClass(MediatorManager.class);
        weld.addBeanClass(InternalChannelRegistry.class);
        weld.addBeanClass(ConfiguredChannelFactory.class);
        weld.addBeanClass(StreamProducer.class);
        weld.addExtension(new ReactiveMessagingExtension());

        weld.addBeanClass(SnsConnector.class);
        weld.disableDiscovery();

        return weld;
    }

    static void addConfig(MapBasedConfig config) {
        if (config != null) {
            config.write();
        } else {
            MapBasedConfig.clear();
        }
    }

    public static void clear() {
        File out = new File("target/test-classes/META-INF/microprofile-config.properties");
        if (out.exists()) {
            out.delete();
        }
    }

    protected void await(int seconds) {

        try {
            TimeUnit.SECONDS.sleep(seconds);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
