package io.smallrye.reactive.messaging.kafka;

import static org.awaitility.Awaitility.await;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import io.debezium.kafka.KafkaCluster;
import io.debezium.util.Testing;
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
import io.vertx.mutiny.core.Vertx;

public class KafkaTestBase {

    protected static KafkaCluster kafka;
    public static final String SERVERS = "localhost:9092";

    public Vertx vertx;

    @BeforeClass
    public static void startKafkaBroker() throws IOException {
        Properties props = new Properties();
        props.setProperty("zookeeper.connection.timeout.ms", "10000");
        File directory = Testing.Files.createTestingDirectory(System.getProperty("java.io.tmpdir"), true);
        kafka = new KafkaCluster().withPorts(2182, 9092).addBrokers(1)
                .usingDirectory(directory)
                .deleteDataUponShutdown(false)
                .withKafkaConfiguration(props)
                .deleteDataPriorToStartup(true)
                .startup();
    }

    @AfterClass
    public static void stopKafkaBroker() {
        if (kafka != null) {
            try {
                kafka.shutdown();
            } catch (Exception e) {
                // Ignore it.
            }
            await().until(() -> !kafka.isRunning());
        }
    }

    @Before
    public void setup() {
        vertx = Vertx.vertx();
    }

    @After
    public void tearDown() {
        vertx.close();
    }

    public void restart(int i) throws IOException, InterruptedException {
        try {
            kafka.shutdown();
        } catch (Exception e) {
            // Ignore me.
        }
        await().until(() -> !kafka.isRunning());
        Thread.sleep(i * 1000);
        kafka.startup();
    }

    public static Weld baseWeld() {
        Weld weld = new Weld();

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

        weld.addBeanClass(KafkaConnector.class);
        weld.disableDiscovery();
        return weld;
    }

    public static void addConfig(MapBasedConfig config) {
        if (config != null) {
            config.write();
        } else {
            MapBasedConfig.clear();
        }
    }

    public HealthCenter getHealth(WeldContainer container) {
        return container.getBeanManager().createInstance().select(HealthCenter.class).get();
    }

    public boolean isReady(WeldContainer container) {
        return getHealth(container).getReadiness().isOk();
    }

    public boolean isLive(WeldContainer container) {
        return getHealth(container).getLiveness().isOk();
    }

}
