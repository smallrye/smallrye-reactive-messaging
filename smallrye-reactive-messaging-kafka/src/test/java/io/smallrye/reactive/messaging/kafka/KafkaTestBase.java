package io.smallrye.reactive.messaging.kafka;

import io.debezium.kafka.KafkaCluster;
import io.debezium.util.Testing;
import io.vertx.reactivex.core.Vertx;
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
    kafka.shutdown();
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
    kafka.shutdown();
    Thread.sleep(i * 1000);
    kafka.startup();
  }

}
