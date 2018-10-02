package io.smallrye.reactive.messaging.mqtt;

import io.reactivex.Flowable;
import io.smallrye.reactive.messaging.MediatorFactory;
import io.smallrye.reactive.messaging.ReactiveMessagingExtension;
import io.smallrye.reactive.messaging.impl.ConfiguredStreamFactory;
import io.smallrye.reactive.messaging.impl.StreamFactoryImpl;
import io.smallrye.reactive.messaging.impl.InternalStreamRegistry;
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
    Map<String, String> config = new HashMap<>();
    config.put("topic", topic);
    config.put("host", address);
    config.put("port", Integer.toString(port));
    MqttSource source = new MqttSource(vertx, config);

    List<MqttMessage> messages = new ArrayList<>();
    Flowable<MqttMessage> flowable = source.getSource();
    flowable.forEach(messages::add);
    AtomicInteger counter = new AtomicInteger();
    new Thread(() ->
      usage.produceIntegers(topic, 10, null,
        counter::getAndIncrement)).start();

    await().atMost(2, TimeUnit.MINUTES).until(() -> messages.size() >= 10);
    assertThat(messages.stream().map(MqttMessage::getPayload)
      .map(bytes -> Integer.valueOf(new String(bytes)))
      .collect(Collectors.toList()))
      .containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
  }

  @Test
  public void testMulticast() {
    String topic = UUID.randomUUID().toString();
    Map<String, String> config = new HashMap<>();
    config.put("topic", topic);
    config.put("host", address);
    config.put("port", Integer.toString(port));
    config.put("multicast", "true");

    MqttSource source = new MqttSource(vertx, config);

    List<MqttMessage> messages1 = new ArrayList<>();
    List<MqttMessage> messages2 = new ArrayList<>();
    Flowable<MqttMessage> flowable = source.getSource();
    flowable.forEach(messages1::add);
    flowable.forEach(messages2::add);

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
    await().until(() -> container.getBeanManager().getExtension(ReactiveMessagingExtension.class).isInitialized());

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
    Weld weld = new Weld();
    weld.addBeanClass(MediatorFactory.class);
    weld.addBeanClass(InternalStreamRegistry.class);
    weld.addBeanClass(StreamFactoryImpl.class);
    weld.addBeanClass(ConfiguredStreamFactory.class);
    weld.addExtension(new ReactiveMessagingExtension());
    weld.addBeanClass(MqttMessagingProvider.class);
    weld.addBeanClass(ConsumptionBean.class);
    weld.disableDiscovery();
    container = weld.initialize();
    return container.getBeanManager().createInstance().select(ConsumptionBean.class).get();
  }

}
