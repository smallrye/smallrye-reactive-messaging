package io.smallrye.reactive.messaging.mqtt;

import io.vertx.reactivex.core.Vertx;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.testcontainers.containers.GenericContainer;

public class MqttTestBase {

  @ClassRule
  public static GenericContainer mosquitto = new GenericContainer("eclipse-mosquitto:1.4.12")
    .withExposedPorts(1883);

  Vertx vertx;
  protected String address;
  protected Integer port;
  protected MqttUsage usage;

  @Before
  public void setup() {
    vertx = Vertx.vertx();
    address = mosquitto.getContainerIpAddress();
    port = mosquitto.getMappedPort(1883);
    System.setProperty("mqtt-host", address);
    System.setProperty("mqtt-port", Integer.toString(port));
    usage = new MqttUsage(address, port);
  }

  @After
  public void tearDown() {
    System.clearProperty("mqtt-host");
    System.clearProperty("mqtt-posrt");
    vertx.close();
    usage.close();
  }


}
