package io.smallrye.reactive.messaging.mqtt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.After;
import org.junit.Test;

import io.reactivex.Flowable;
import io.smallrye.reactive.messaging.MediatorFactory;
import io.smallrye.reactive.messaging.extension.MediatorManager;
import io.smallrye.reactive.messaging.extension.ReactiveMessagingExtension;
import io.smallrye.reactive.messaging.impl.ConfiguredStreamFactory;
import io.smallrye.reactive.messaging.impl.InternalStreamRegistry;
import io.smallrye.reactive.messaging.impl.StreamFactoryImpl;

public class SecureMqttSourceTest extends SecureMqttTestBase {

  private WeldContainer container;

  @After
  public void cleanup() {
    if (container != null) {
      container.close();
    }
  }


  @Test
  public void testSecureSource() {
    String topic = UUID.randomUUID().toString();
    Map<String, String> config = new HashMap<>();
    config.put("topic", topic);
    config.put("host", address);
    config.put("port", Integer.toString(port));
    config.put("username", "user");
    config.put("password", "foo");
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
  public void testABeanConsumingTheMQTTMessagesWithAuthentication() {
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
    Weld weld = MqttTestBase.baseWeld();
    weld.addBeanClass(ConsumptionBean.class);
    container = weld.initialize();
    return container.getBeanManager().createInstance().select(ConsumptionBean.class).get();
  }

}
