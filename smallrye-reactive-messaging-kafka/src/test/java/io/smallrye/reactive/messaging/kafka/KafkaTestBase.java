package io.smallrye.reactive.messaging.kafka;

import io.debezium.kafka.KafkaCluster;
import io.debezium.util.Testing;
import io.smallrye.reactive.messaging.MediatorFactory;
import io.smallrye.reactive.messaging.extension.MediatorManager;
import io.smallrye.reactive.messaging.extension.ReactiveMessagingExtension;
import io.smallrye.reactive.messaging.impl.ConfiguredChannelFactory;
import io.smallrye.reactive.messaging.impl.InternalChannelRegistry;
import io.vertx.reactivex.core.Vertx;

import org.jboss.weld.environment.se.Weld;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

public class KafkaTestBase {

  private static KafkaCluster kafka;

  Vertx vertx;

  @BeforeClass
  public static void beforeClass() throws IOException {
    Properties props = new Properties();
    props.setProperty("zookeeper.connection.timeout.ms", "10000");
    File directory = Testing.Files.createTestingDirectory(System.getProperty("java.io.tmpdir"), true);
    kafka = new KafkaCluster().withPorts(2182, 9092).addBrokers(1)
      .usingDirectory(directory)
      .deleteDataUponShutdown(true)
      .withKafkaConfiguration(props)
      .deleteDataPriorToStartup(true)
      .startup();
  }

  @AfterClass
  public static void afterClass() {
    try {
      kafka.shutdown();
    } catch (Exception e) {
      // Ignore it.
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
    Thread.sleep(i * 1000);
    kafka.startup();
  }

  static Weld baseWeld() {
    Weld weld = new Weld();
    weld.addBeanClass(MediatorFactory.class);
    weld.addBeanClass(MediatorManager.class);
    weld.addBeanClass(InternalChannelRegistry.class);
    weld.addBeanClass(ConfiguredChannelFactory.class);
    weld.addExtension(new ReactiveMessagingExtension());
    weld.addBeanClass(KafkaConnector.class);
    weld.disableDiscovery();
    return weld;
  }
  
}
