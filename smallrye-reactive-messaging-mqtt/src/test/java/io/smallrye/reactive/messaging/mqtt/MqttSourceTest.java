package io.smallrye.reactive.messaging.mqtt;

import io.smallrye.reactive.messaging.extension.MediatorManager;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.After;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class MqttSourceTest extends MqttTestBase {

  private WeldContainer container;

  @After
  public void cleanup() {
    if (container != null) {
      container.close();
    }
  }


  @Test
  public void testSource() {
    String topic = UUID.randomUUID().toString();
    Map<String, Object> config = new HashMap<>();
    config.put("topic", topic);
    config.put("host", address);
    config.put("port", port);
    MqttSource source = new MqttSource(vertx, new MapBasedConfig(config));

    List<MqttMessage> messages = new ArrayList<>();
    PublisherBuilder<MqttMessage> stream = source.getSource();
    stream.forEach(messages::add).run();
    await().until(source::isSubscribed);
    AtomicInteger counter = new AtomicInteger();
    new Thread(() ->
      usage.produceIntegers(topic, 10, null,
        counter::getAndIncrement)
    ).start();

    await().atMost(2, TimeUnit.MINUTES).until(() -> messages.size() >= 10);
    assertThat(messages.stream().map(MqttMessage::getPayload)
      .map(bytes -> Integer.valueOf(new String(bytes)))
      .collect(Collectors.toList()))
      .containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
  }

  @Test
  public void testBroadcast() {
    String topic = UUID.randomUUID().toString();
    Map<String, Object> config = new HashMap<>();
    config.put("topic", topic);
    config.put("host", address);
    config.put("port", port);
    config.put("broadcast", true);

    MqttSource source = new MqttSource(vertx, new MapBasedConfig(config));

    List<MqttMessage> messages1 = new ArrayList<>();
    List<MqttMessage> messages2 = new ArrayList<>();
    PublisherBuilder<MqttMessage> stream = source.getSource();
    stream.forEach(messages1::add).run();
    stream.forEach(messages2::add).run();

    await().until(source::isSubscribed);

    AtomicInteger counter = new AtomicInteger();
    new Thread(() ->
      usage.produceIntegers(topic, 10, null,
        counter::getAndIncrement)).start();

    await().atMost(2, TimeUnit.MINUTES).until(() -> messages1.size() >= 10);
    await().atMost(2, TimeUnit.MINUTES).until(() -> messages2.size() >= 10);
    assertThat(messages1.stream().map(MqttMessage::getPayload)
      .map(bytes -> Integer.valueOf(new String(bytes)))
      .collect(Collectors.toList()))
      .containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

    assertThat(messages2.stream().map(MqttMessage::getPayload)
      .map(bytes -> Integer.valueOf(new String(bytes)))
      .collect(Collectors.toList()))
      .containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
  }

  @Test
  public void testABeanConsumingTheMQTTMessages() {
    ConsumptionBean bean = deploy();
    await().until(() -> container.select(MediatorManager.class).get().isInitialized());

    List<Integer> list = bean.getResults();
    assertThat(list).isEmpty();

    AtomicInteger counter = new AtomicInteger();
    new Thread(() ->
      usage.produceIntegers("data", 10, null, counter::getAndIncrement))
      .start();

    await().atMost(2, TimeUnit.MINUTES).until(() -> list.size() >= 10);
    assertThat(list).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
  }

  private ConsumptionBean deploy() {
    Weld weld = baseWeld();
    weld.addBeanClass(ConsumptionBean.class);
    container = weld.initialize();
    return container.getBeanManager().createInstance().select(ConsumptionBean.class).get();
  }

}
