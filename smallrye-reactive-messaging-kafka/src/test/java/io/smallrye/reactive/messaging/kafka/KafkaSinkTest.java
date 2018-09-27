package io.smallrye.reactive.messaging.kafka;

import io.reactivex.Flowable;
import io.smallrye.reactive.messaging.MediatorFactory;
import io.smallrye.reactive.messaging.ReactiveMessagingExtension;
import io.smallrye.reactive.messaging.impl.ConfiguredStreamFactory;
import io.smallrye.reactive.messaging.impl.StreamFactoryImpl;
import io.smallrye.reactive.messaging.impl.StreamRegistryImpl;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class KafkaSinkTest extends KafkaTestBase {


  private WeldContainer container;

  @After
  public void cleanup() {
    if (container != null) {
      container.close();
    }
  }

  @Test
  public void testSinkUsingInteger() throws InterruptedException {
    KafkaUsage usage = new KafkaUsage();
    String topic = UUID.randomUUID().toString();
    CountDownLatch latch = new CountDownLatch(1);
    AtomicInteger expected = new AtomicInteger(2);
    usage.consumeIntegers(topic, 10, 10, TimeUnit.SECONDS,
      latch::countDown,
      (k, v) -> v == expected.getAndIncrement());


    Map<String, String> config = newCommonConfig();
    config.put("topic", topic);
    config.put("value.serializer", IntegerSerializer.class.getName());
    config.put("value.deserializer", IntegerDeserializer.class.getName());
    KafkaSink sink = new KafkaSink(vertx, config);

    Flowable.range(0, 10)
      .map(Message::of)
      .subscribe(sink.getSubscriber());

    assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
  }

  @Test
  public void testSinkUsingString() throws InterruptedException {
    KafkaUsage usage = new KafkaUsage();
    String topic = UUID.randomUUID().toString();
    CountDownLatch latch = new CountDownLatch(1);
    AtomicInteger expected = new AtomicInteger(2);
    usage.consumeIntegers(topic, 10, 10, TimeUnit.SECONDS,
      latch::countDown,
      (k, v) -> v == expected.getAndIncrement());


    Map<String, String> config = newCommonConfig();
    config.put("topic", topic);
    config.put("value.serializer", IntegerSerializer.class.getName());
    config.put("value.deserializer", IntegerDeserializer.class.getName());
    KafkaSink sink = new KafkaSink(vertx, config);

    Flowable.range(0, 10)
      .map(i -> Integer.toString(i))
      .map(Message::of)
      .subscribe(sink.getSubscriber());

    assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
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
  public void testABeanProducingMessagesSentToKafka() throws InterruptedException {
    Weld weld = new Weld();

    weld.addBeanClass(MediatorFactory.class);
    weld.addBeanClass(StreamRegistryImpl.class);
    weld.addBeanClass(StreamFactoryImpl.class);
    weld.addBeanClass(ConfiguredStreamFactory.class);
    weld.addExtension(new ReactiveMessagingExtension());

    weld.addBeanClass(KafkaMessagingProvider.class);
    weld.addBeanClass(ConsumptionBean.class);

    weld.disableDiscovery();

    container = weld.initialize();

    KafkaUsage usage = new KafkaUsage();
    CountDownLatch latch = new CountDownLatch(1);
    AtomicInteger expected = new AtomicInteger(2);
    usage.consumeIntegers("sink", 10, 10, TimeUnit.SECONDS,
      latch::countDown,
      (k, v) -> v == expected.getAndIncrement());

    assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
  }


}
