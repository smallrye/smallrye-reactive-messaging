package io.smallrye.reactive.messaging.mqtt;

import io.reactivex.Flowable;
import io.smallrye.reactive.messaging.MediatorFactory;
import io.smallrye.reactive.messaging.extension.MediatorManager;
import io.smallrye.reactive.messaging.extension.ReactiveMessagingExtension;
import io.smallrye.reactive.messaging.impl.ConfiguredStreamFactory;
import io.smallrye.reactive.messaging.impl.InternalStreamRegistry;
import io.smallrye.reactive.messaging.impl.StreamFactoryImpl;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.After;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import repeat.Repeat;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.core.Is.is;

public class MqttSinkTest extends MqttTestBase {

  private WeldContainer container;

  @After
  public void cleanup() {
    if (container != null) {
      container.close();
    }
  }

  @Test
  public void testSinkUsingInteger() throws InterruptedException {
    String topic = UUID.randomUUID().toString();
    CountDownLatch latch = new CountDownLatch(1);
    AtomicInteger expected = new AtomicInteger(0);
    usage.consumeIntegers(topic, 10, 10, TimeUnit.SECONDS,
      latch::countDown,
      v -> expected.getAndIncrement());


    Map<String, String> config = new HashMap<>();
    config.put("topic", topic);
    config.put("host", address);
    config.put("port", Integer.toString(port));
    MqttSink sink = new MqttSink(vertx, config);

    Subscriber subscriber = sink.getSubscriber();
    Flowable.range(0, 10)
      .map(v -> (Message) Message.of(v))
      .subscribe(subscriber);

    assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
    await().untilAtomic(expected, is(10));
    assertThat(expected).hasValue(10);
  }

  @Test
  public void testSinkUsingString() throws InterruptedException {
    String topic = UUID.randomUUID().toString();
    CountDownLatch latch = new CountDownLatch(1);
    AtomicInteger expected = new AtomicInteger(0);
    usage.consumeStrings(topic, 10, 10, TimeUnit.SECONDS,
      latch::countDown,
      v -> expected.getAndIncrement());

    Map<String, String> config = new HashMap<>();
    config.put("topic", topic);
    config.put("host", address);
    config.put("port", Integer.toString(port));
    MqttSink sink = new MqttSink(vertx, config);

    Subscriber subscriber = sink.getSubscriber();
    Flowable.range(0, 10)
      .map(i -> Integer.toString(i))
      .map(Message::of)
      .subscribe(subscriber);

    assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
    await().untilAtomic(expected, is(10));
    assertThat(expected).hasValue(10);
  }

  @Test
  @Repeat(times = 5)
  public void testABeanProducingMessagesSentToMQTT() throws InterruptedException {
    Weld weld = baseWeld();
    weld.addBeanClass(ProducingBean.class);

    CountDownLatch latch = new CountDownLatch(10);
    usage.consumeIntegers("sink", 10, 10, TimeUnit.SECONDS,
      null,
      v -> latch.countDown());

    container = weld.initialize();
    assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
  }

}
