package io.smallrye.reactive.messaging.kafka;

import io.reactivex.Flowable;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.After;
import org.junit.Test;
import org.reactivestreams.Subscriber;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

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
    AtomicInteger expected = new AtomicInteger(0);
    usage.consumeIntegers(topic, 10, 10, TimeUnit.SECONDS,
      latch::countDown,
      (k, v) -> expected.getAndIncrement());


    Map<String, Object> config = getConfig();
    config.put("topic", topic);
    config.put("value.serializer", IntegerSerializer.class.getName());
    config.put("value.deserializer", IntegerDeserializer.class.getName());
    config.put("partition", 0);
    KafkaSink sink = new KafkaSink(vertx, new MapBasedConfig(config));

    Flowable.range(0, 10)
      .map(Message::of)
      .subscribe((Subscriber) sink.getSink().build());

    assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
    assertThat(expected).hasValue(10);
  }

  @Test
  public void testSinkUsingIntegerAndChannelName() throws InterruptedException {
    KafkaUsage usage = new KafkaUsage();
    String topic = UUID.randomUUID().toString();
    CountDownLatch latch = new CountDownLatch(1);
    AtomicInteger expected = new AtomicInteger(0);
    usage.consumeIntegers(topic, 10, 10, TimeUnit.SECONDS,
      latch::countDown,
      (k, v) -> expected.getAndIncrement());


    Map<String, Object> config = getConfig();
    config.put("channel-name", topic);
    config.put("value.serializer", IntegerSerializer.class.getName());
    config.put("value.deserializer", IntegerDeserializer.class.getName());
    config.put("partition", 0);
    KafkaSink sink = new KafkaSink(vertx, new MapBasedConfig(config));

    Flowable.range(0, 10)
      .map(Message::of)
      .subscribe((Subscriber) sink.getSink().build());

    assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
    assertThat(expected).hasValue(10);
  }

  @Test
  public void testSinkUsingString() throws InterruptedException {
    KafkaUsage usage = new KafkaUsage();
    String topic = UUID.randomUUID().toString();
    CountDownLatch latch = new CountDownLatch(1);
    AtomicInteger expected = new AtomicInteger(0);
    usage.consumeStrings(topic, 10, 10, TimeUnit.SECONDS,
      latch::countDown,
      (k, v) -> expected.getAndIncrement());


    Map<String, Object> config = getConfig();
    config.put("topic", topic);
    config.put("value.serializer", StringSerializer.class.getName());
    config.put("value.deserializer", StringDeserializer.class.getName());
    config.put("partition", 0);
    KafkaSink sink = new KafkaSink(vertx, new MapBasedConfig(config));

    Flowable.range(0, 10)
      .map(i -> Integer.toString(i))
      .map(Message::of)
      .subscribe((Subscriber) sink.getSink().build());

    assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
    assertThat(expected).hasValue(10);
  }

  private Map<String, Object> getConfig() {
    Map<String, Object> config = new HashMap<>();
    config.put("bootstrap.servers", "localhost:9092");
    config.put("key.serializer", StringSerializer.class.getName());
    config.put("acks", "1");
    return config;
  }


  @Test
  public void testABeanProducingMessagesSentToKafka() throws InterruptedException {
    Weld weld = baseWeld();
    weld.addBeanClass(ProducingBean.class);
    container = weld.initialize();

    KafkaUsage usage = new KafkaUsage();
    CountDownLatch latch = new CountDownLatch(1);
    AtomicInteger expected = new AtomicInteger(0);
    usage.consumeIntegers("output", 10, 10, TimeUnit.SECONDS,
      latch::countDown,
      (k, v) -> expected.getAndIncrement());

    assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
    assertThat(expected).hasValue(10);
  }

  @Test
  public void testABeanProducingKafkaMessagesSentToKafka() throws InterruptedException {
    Weld weld = baseWeld();
    weld.addBeanClass(ProducingKafkaMessageBean.class);
    container = weld.initialize();

    KafkaUsage usage = new KafkaUsage();
    CountDownLatch latch = new CountDownLatch(1);
    AtomicInteger expected = new AtomicInteger(0);
    List<String> keys = new ArrayList<>();
    List<String> headers = new ArrayList<>();
    usage.consumeIntegers("output-2", 10, 10, TimeUnit.SECONDS,
      latch::countDown,
      record -> {
        keys.add(record.key());
        String count = new String(record.headers().lastHeader("count").value());
        headers.add(count);
        expected.getAndIncrement();
      });

    assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
    assertThat(expected).hasValue(10);
    assertThat(keys).containsExactly("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
    assertThat(headers).containsExactly("1", "2", "3", "4", "5", "6", "7", "8", "9", "10");
  }


}
