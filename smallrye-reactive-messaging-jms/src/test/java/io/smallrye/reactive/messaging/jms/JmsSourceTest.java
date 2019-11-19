package io.smallrye.reactive.messaging.jms;

import io.smallrye.reactive.messaging.jms.support.JmsTestBase;
import io.smallrye.reactive.messaging.jms.support.MapBasedConfig;
import org.apache.activemq.artemis.jms.client.ActiveMQJMSConnectionFactory;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jms.*;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class JmsSourceTest extends JmsTestBase {

  private JMSContext jms;
  private ActiveMQJMSConnectionFactory factory;

  @Before
  public void init() {
    factory = new ActiveMQJMSConnectionFactory(
      "tcp://localhost:61616",
      null, null);
    jms = factory.createContext();
  }

  @After
  public void close() {
    jms.close();
    factory.close();
  }

  @Test
  public void testWithString() throws JMSException {
    WeldContainer container = prepare();

    RawMessageConsumerBean bean = container.select(RawMessageConsumerBean.class).get();
    assertThat(bean.messages()).isEmpty();

    Queue q = jms.createQueue("queue-one");
    JMSProducer producer = jms.createProducer();
    TextMessage message = jms.createTextMessage("hello");
    message.setStringProperty("string", "value");
    message.setBooleanProperty("bool", true);
    message.setLongProperty("long", 100L);
    message.setByteProperty("byte", (byte) 5);
    message.setFloatProperty("float",  5.5f);
    message.setDoubleProperty("double",  10.3);
    message.setIntProperty("int", 23);
    message.setObjectProperty("object", "yop");
    message.setShortProperty("short", (short) 3);
    producer.send(q, message);

    await().until(() -> bean.messages().size() == 1);
    ReceivedJmsMessage<?> receivedJmsMessage = bean.messages().get(0);
    assertThat(receivedJmsMessage.getPayload()).isEqualTo("hello");
    assertThat(receivedJmsMessage.getBody(String.class)).isEqualTo("hello");
    assertThat(receivedJmsMessage.propertyExists("string")).isTrue();
    assertThat(receivedJmsMessage.propertyExists("missing")).isFalse();
    assertThat(receivedJmsMessage.getStringProperty("string")).isEqualTo("value");
    assertThat(receivedJmsMessage.getBooleanProperty("bool")).isTrue();
    assertThat(receivedJmsMessage.getLongProperty("long")).isEqualTo(100L);
    assertThat(receivedJmsMessage.getByteProperty("byte")).isEqualTo((byte) 5);
    assertThat(receivedJmsMessage.getFloatProperty("float")).isEqualTo(5.5f);
    assertThat(receivedJmsMessage.getDoubleProperty("double")).isEqualTo(10.3);
    assertThat(receivedJmsMessage.getIntProperty("int")).isEqualTo(23);
    assertThat(receivedJmsMessage.getObjectProperty("object")).isInstanceOf(String.class);
    assertThat(((String) receivedJmsMessage.getObjectProperty("object"))).isEqualTo("yop");
    assertThat(receivedJmsMessage.getShortProperty("short")).isEqualTo((short) 3);
  }

  @Test
  public void testWithLong() {
    WeldContainer container = prepare();

    RawMessageConsumerBean bean = container.select(RawMessageConsumerBean.class).get();
    assertThat(bean.messages()).isEmpty();

    Queue q = jms.createQueue("queue-one");
    JMSProducer producer = jms.createProducer();
    producer.send(q, 10000L);

    await().until(() -> bean.messages().size() == 1);
    ReceivedJmsMessage<?> receivedJmsMessage = bean.messages().get(0);
    assertThat(receivedJmsMessage.getPayload()).isEqualTo(10000L);
  }

  @Test
  public void testWithDurableTopic() {
    Map<String, Object> map = new HashMap<>();
    map.put("mp.messaging.incoming.jms.connector", JmsConnector.CONNECTOR_NAME);
    map.put("mp.messaging.incoming.jms.destination", "my-topic");
    map.put("mp.messaging.incoming.jms.durable", "true");
    map.put("mp.messaging.incoming.jms.client-id", "me");
    map.put("mp.messaging.incoming.jms.destination-type", "topic");
    MapBasedConfig config = new MapBasedConfig(map);
    addConfig(config);
    WeldContainer container = deploy(RawMessageConsumerBean.class);
    RawMessageConsumerBean bean = container.select(RawMessageConsumerBean.class).get();
    assertThat(bean.messages()).isEmpty();


    Topic q = jms.createTopic("my-topic");
    JMSProducer producer = jms.createProducer();
    String uuid = UUID.randomUUID().toString();
    producer.send(q, uuid);

    await().until(() -> bean.messages().size() == 1);
    ReceivedJmsMessage<?> receivedJmsMessage = bean.messages().get(0);
    assertThat(receivedJmsMessage.getPayload()).isEqualTo(uuid);
  }

  private WeldContainer prepare() {
    Map<String, Object> map = new HashMap<>();
    map.put("mp.messaging.incoming.jms.connector", JmsConnector.CONNECTOR_NAME);
    map.put("mp.messaging.incoming.jms.destination", "queue-one");
    MapBasedConfig config = new MapBasedConfig(map);
    addConfig(config);
    return deploy(RawMessageConsumerBean.class);
  }
}
