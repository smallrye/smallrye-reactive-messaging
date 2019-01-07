package io.smallrye.reactive.messaging.amqp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.awaitility.Awaitility.await;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import io.reactivex.Flowable;
import io.smallrye.reactive.messaging.extension.MediatorManager;

public class AmqpSourceTest extends AmqpTestBase {

  private AmqpMessagingProvider provider;

  private WeldContainer container;

  @After
  public void cleanup() {
    if (container != null) {
      container.close();
    }
    if (provider != null) {
      provider.close();
    }
  }

  @Test
  public void testSource() {
    String topic = UUID.randomUUID().toString();
    Map<String, String> config = getConfig(topic);
    config.put("ttl", "10000");

    provider = new AmqpMessagingProvider(vertx);
    provider.configure();

    List<Message> messages = new ArrayList<>();
    AmqpSource source = provider.getSource(config).toCompletableFuture().join();
    //noinspection ResultOfMethodCallIgnored
    Flowable.fromPublisher(source.publisher())
      .subscribe(
        messages::add,
        Throwable::printStackTrace
      );
    await().until(source::isOpen);

    AtomicInteger counter = new AtomicInteger();
    new Thread(() ->
      usage.produceTenIntegers(topic,
        counter::getAndIncrement)).start();

    await().atMost(2, TimeUnit.MINUTES).until(() -> messages.size() >= 10);
    assertThat(messages.stream().map(Message::getPayload)
      .collect(Collectors.toList()))
      .containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
  }

  @Test
  public void testBroadcast() {
    String topic = UUID.randomUUID().toString();
    Map<String, String> config = new HashMap<>();
    config.put("address", topic);
    config.put("host", address);
    config.put("port", Integer.toString(port));
    config.put("broadcast", "true");
    config.put("username", "artemis");
    config.put("password", new String("simetraehcapa".getBytes()));

    AmqpMessagingProvider provider = new AmqpMessagingProvider(vertx);
    provider.configure();
    AmqpSource source = provider.getSource(config).toCompletableFuture().join();

    List<Message> messages1 = new ArrayList<>();
    List<Message> messages2 = new ArrayList<>();
    Flowable<? extends Message> flowable = Flowable.fromPublisher(source.publisher());

    //noinspection ResultOfMethodCallIgnored
    flowable
      .forEach(messages1::add);
    //noinspection ResultOfMethodCallIgnored
    flowable
      .forEach(messages2::add);

    await().until(source::isOpen);

    AtomicInteger counter = new AtomicInteger();
    new Thread(() ->
      usage.produceTenIntegers(topic,
        counter::getAndIncrement)).start();

    await().atMost(2, TimeUnit.MINUTES).until(() -> messages1.size() >= 10);
    await().atMost(2, TimeUnit.MINUTES).until(() -> messages2.size() >= 10);
    assertThat(messages1.stream().map(Message::getPayload)
      .collect(Collectors.toList()))
      .containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

    assertThat(messages2.stream().map(Message::getPayload)
      .collect(Collectors.toList()))
      .containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
  }

  @Test
  public void testABeanConsumingTheAMQPMessages() {
    ConsumptionBean bean = deploy();

    List<Integer> list = bean.getResults();
    assertThat(list).isEmpty();

    AtomicInteger counter = new AtomicInteger();
    usage.produceTenIntegers("data", counter::getAndIncrement);

    await().atMost(2, TimeUnit.MINUTES).until(() -> list.size() >= 10);
    assertThat(list).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
  }

  private ConsumptionBean deploy() {
    Weld weld = new Weld();
    weld.addBeanClass(AmqpMessagingProvider.class);
    weld.addBeanClass(ConsumptionBean.class);
    container = weld.initialize();
    await().until(() -> container.select(MediatorManager.class).get().isInitialized());
    return container.getBeanManager().createInstance().select(ConsumptionBean.class).get();
  }

  @Test
  public void testSourceWithBinaryContent() {
    String topic = UUID.randomUUID().toString();
    Map<String, String> config = getConfig(topic);
    provider = new AmqpMessagingProvider(vertx);
    provider.configure();

    List<Message<byte[]>> messages = new ArrayList<>();
    AmqpSource source = provider.getSource(config).toCompletableFuture().join();
    //noinspection ResultOfMethodCallIgnored
    Flowable.fromPublisher(source.publisher())
      .subscribe(
        messages::add,
        Throwable::printStackTrace
      );
    await().until(source::isOpen);

    usage.produce(topic, 1, () -> new AmqpValue(new Binary("hello".getBytes())));

    await().atMost(2, TimeUnit.MINUTES).until(() -> !messages.isEmpty());
    assertThat(messages.stream().map(Message::getPayload)
      .collect(Collectors.toList()))
      .containsExactly("hello".getBytes());
  }

  @Test
  public void testSourceWithMapContent() {
    String topic = UUID.randomUUID().toString();
    Map<String, String> config = getConfig(topic);
    provider = new AmqpMessagingProvider(vertx);
    provider.configure();

    List<Message<Map<String, String>>> messages = new ArrayList<>();
    AmqpSource source = provider.getSource(config).toCompletableFuture().join();
    //noinspection ResultOfMethodCallIgnored
    Flowable.fromPublisher(source.publisher())
      .subscribe(
        messages::add,
        Throwable::printStackTrace
      );
    await().until(source::isOpen);

    Map<String, String> map = new HashMap<>();
    map.put("hello", "world");
    map.put("some", "content");
    usage.produce(topic, 1, () -> new AmqpValue(map));

    await().atMost(2, TimeUnit.MINUTES).until(() -> !messages.isEmpty());
    Map<String, String> result = messages.get(0).getPayload();
    assertThat(result)
      .containsOnly(entry("hello", "world"), entry("some", "content"));
  }

  @Test
  public void testSourceWithListContent() {
    String topic = UUID.randomUUID().toString();
    Map<String, String> config = getConfig(topic);
    provider = new AmqpMessagingProvider(vertx);
    provider.configure();

    List<Message<List<String>>> messages = new ArrayList<>();
    AmqpSource source = provider.getSource(config).toCompletableFuture().join();
    //noinspection ResultOfMethodCallIgnored
    Flowable.fromPublisher(source.publisher())
      .subscribe(
        messages::add,
        Throwable::printStackTrace
      );
    await().until(source::isOpen);

    List<String> list = new ArrayList<>();
    list.add("hello");
    list.add("world");
    usage.produce(topic, 1, () -> new AmqpValue(list));

    await().atMost(2, TimeUnit.MINUTES).until(() -> !messages.isEmpty());
    List<String> result = messages.get(0).getPayload();
    assertThat(result)
      .containsExactly("hello", "world");
  }

  @Test
  public void testSourceWithSeqContent() {
    String topic = UUID.randomUUID().toString();
    Map<String, String> config = getConfig(topic);
    provider = new AmqpMessagingProvider(vertx);
    provider.configure();

    List<Message<List<String>>> messages = new ArrayList<>();
    AmqpSource source = provider.getSource(config).toCompletableFuture().join();
    //noinspection ResultOfMethodCallIgnored
    Flowable.fromPublisher(source.publisher())
      .subscribe(
        messages::add,
        Throwable::printStackTrace
      );
    await().until(source::isOpen);

    List<String> list = new ArrayList<>();
    list.add("hello");
    list.add("world");
    usage.produce(topic, 1, () -> new AmqpSequence(list));

    await().atMost(2, TimeUnit.MINUTES).until(() -> !messages.isEmpty());
    List<String> result = messages.get(0).getPayload();
    assertThat(result)
      .containsOnly("hello", "world");
  }

  @Test
  public void testSourceWithDataContent() {
    String topic = UUID.randomUUID().toString();
    Map<String, String> config = getConfig(topic);
    provider = new AmqpMessagingProvider(vertx);
    provider.configure();

    List<Message<byte[]>> messages = new ArrayList<>();
    AmqpSource source = provider.getSource(config).toCompletableFuture().join();
    //noinspection ResultOfMethodCallIgnored
    Flowable.fromPublisher(source.publisher())
      .subscribe(
        messages::add,
        Throwable::printStackTrace
      );
    await().until(source::isOpen);

    List<String> list = new ArrayList<>();
    list.add("hello");
    list.add("world");
    usage.produce(topic, 1, () -> new Data(new Binary(list.toString().getBytes())));

    await().atMost(2, TimeUnit.MINUTES).until(() -> !messages.isEmpty());
    byte[] result = messages.get(0).getPayload();
    assertThat(new String(result))
      .isEqualTo(list.toString());
  }

  @NotNull
  private Map<String, String> getConfig(String topic) {
    Map<String, String> config = new HashMap<>();
    config.put("address", topic);
    config.put("host", address);
    config.put("port", Integer.toString(port));
    config.put("username", "artemis");
    config.put("password", new String("simetraehcapa".getBytes()));
    return config;
  }

}
