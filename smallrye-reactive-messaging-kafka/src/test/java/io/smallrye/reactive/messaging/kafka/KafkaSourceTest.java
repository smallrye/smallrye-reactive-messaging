package io.smallrye.reactive.messaging.kafka;

import io.smallrye.reactive.messaging.MediatorFactory;
import io.smallrye.reactive.messaging.ReactiveMessagingExtension;
import io.smallrye.reactive.messaging.impl.ConfiguredStreamFactory;
import io.smallrye.reactive.messaging.impl.StreamFactoryImpl;
import io.smallrye.reactive.messaging.impl.InternalStreamRegistry;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class KafkaSourceTest extends KafkaTestBase {


  private WeldContainer container;

  @After
  public void cleanup() {
    if (container != null) {
      container.close();
    }
  }

  @Test
  public void testSource() throws IOException {
    KafkaUsage usage = new KafkaUsage();
    String topic = UUID.randomUUID().toString();
    Map<String, String> config = newCommonConfig();
    config.put("topic", topic);
    config.put("value.serializer", IntegerSerializer.class.getName());
    config.put("value.deserializer", IntegerDeserializer.class.getName());
    KafkaSource source = new KafkaSource(vertx, config);

    List<KafkaMessage> messages = new ArrayList<>();
    source.getSource().forEach(messages::add);

    AtomicInteger counter = new AtomicInteger();
    new Thread(() ->
      usage.produceIntegers(10, null,
        () -> new ProducerRecord<>(topic, counter.getAndIncrement()))).start();

    await().atMost(2, TimeUnit.MINUTES).until(() -> messages.size() >= 10);
    assertThat(messages.stream().map(KafkaMessage::getPayload).collect(Collectors.toList())).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
  }


  @Test
  public void testMulticastSource() {
    KafkaUsage usage = new KafkaUsage();
    String topic = UUID.randomUUID().toString();
    Map<String, String> config = newCommonConfig();
    config.put("topic", topic);
    config.put("value.serializer", IntegerSerializer.class.getName());
    config.put("value.deserializer", IntegerDeserializer.class.getName());
    config.put("multicast", "true");
    KafkaSource source = new KafkaSource(vertx, config);

    List<KafkaMessage> messages1 = new ArrayList<>();
    List<KafkaMessage> messages2 = new ArrayList<>();
    source.getSource().forEach(messages1::add);
    source.getSource().forEach(messages2::add);

    AtomicInteger counter = new AtomicInteger();
    new Thread(() ->
      usage.produceIntegers(10, null,
        () -> new ProducerRecord<>(topic, counter.getAndIncrement()))).start();

    await().atMost(2, TimeUnit.MINUTES).until(() -> messages1.size() >= 10);
    await().atMost(2, TimeUnit.MINUTES).until(() -> messages2.size() >= 10);
    assertThat(messages1.stream().map(KafkaMessage::getPayload).collect(Collectors.toList())).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    assertThat(messages2.stream().map(KafkaMessage::getPayload).collect(Collectors.toList())).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
  }

  @Test
  public void testRetry() throws IOException, InterruptedException {
    KafkaUsage usage = new KafkaUsage();
    String topic = UUID.randomUUID().toString();
    Map<String, String> config = newCommonConfig();
    config.put("topic", topic);
    config.put("value.serializer", IntegerSerializer.class.getName());
    config.put("value.deserializer", IntegerDeserializer.class.getName());
    config.put("retry", "true");
    config.put("retry-attempts", "100");

    KafkaSource source = new KafkaSource(vertx, config);
    List<KafkaMessage> messages1 = new ArrayList<>();
    source.getSource().forEach(messages1::add);

    AtomicInteger counter = new AtomicInteger();
    new Thread(() ->
      usage.produceIntegers(10, null,
        () -> new ProducerRecord<>(topic, counter.getAndIncrement()))).start();

    await().atMost(2, TimeUnit.MINUTES).until(() -> messages1.size() >= 10);

    restart(5);

    new Thread(() ->
      usage.produceIntegers(10, null,
        () -> new ProducerRecord<>(topic, counter.getAndIncrement()))).start();

    await().atMost(2, TimeUnit.MINUTES).until(() -> messages1.size() >= 20);
  }

  private Map<String, String> newCommonConfig() {
    String randomId = UUID.randomUUID().toString();
    Map<String, String> config = new HashMap<>();
    config.put("bootstrap.servers", "localhost:9092");
    config.put("group.id", randomId);
    config.put("key.deserializer", StringDeserializer.class.getName());
    config.put("key.serializer", StringSerializer.class.getName());
    config.put("enable.auto.commit", "false");
    config.put("auto.offset.reset", "earliest");
    return config;
  }


  @Test
  public void testABeanConsumingTheKafkaMessages() {
    ConsumptionBean bean = deploy();
    KafkaUsage usage = new KafkaUsage();
    List<Integer> list = bean.getResults();
    assertThat(list).isEmpty();
    AtomicInteger counter = new AtomicInteger();
    new Thread(() ->
      usage.produceIntegers(10, null,
        () -> new ProducerRecord<>("data", counter.getAndIncrement()))).start();

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
    weld.addBeanClass(KafkaMessagingProvider.class);
    weld.addBeanClass(ConsumptionBean.class);
    weld.disableDiscovery();
    container = weld.initialize();
    return container.getBeanManager().createInstance().select(ConsumptionBean.class).get();
  }

}
