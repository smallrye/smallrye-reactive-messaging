package io.smallrye.reactive.messaging.mqtt;

import io.smallrye.reactive.messaging.MediatorFactory;
import io.smallrye.reactive.messaging.extension.MediatorManager;
import io.smallrye.reactive.messaging.extension.ReactiveMessagingExtension;
import io.smallrye.reactive.messaging.impl.ConfiguredStreamFactory;
import io.smallrye.reactive.messaging.impl.InternalStreamRegistry;
import io.vertx.reactivex.core.Vertx;

import org.jboss.weld.environment.se.Weld;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.testcontainers.containers.GenericContainer;
import repeat.RepeatRule;

public class MqttTestBase {

  @ClassRule
  public static GenericContainer mosquitto = new GenericContainer("eclipse-mosquitto:1.5.5")
    .withExposedPorts(1883);

  Vertx vertx;
  protected String address;
  protected Integer port;
  protected MqttUsage usage;

  @Rule
  public RepeatRule rule = new RepeatRule();

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
    System.clearProperty("mqtt-post");
    vertx.close();
    usage.close();
  }


  static Weld baseWeld() {
    Weld weld = new Weld();
    weld.disableDiscovery();
    weld.addBeanClass(MediatorFactory.class);
    weld.addBeanClass(MediatorManager.class);
    weld.addBeanClass(InternalStreamRegistry.class);
    weld.addBeanClass(ConfiguredStreamFactory.class);
    weld.addExtension(new ReactiveMessagingExtension());
    weld.addBeanClass(MqttConnector.class);
    return weld;
  }

}
