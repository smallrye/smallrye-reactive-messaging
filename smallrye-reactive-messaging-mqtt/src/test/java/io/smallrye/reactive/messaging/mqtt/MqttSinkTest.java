package io.smallrye.reactive.messaging.mqtt;

import io.reactivex.Flowable;
import io.smallrye.reactive.messaging.MediatorFactory;
import io.smallrye.reactive.messaging.ReactiveMessagingExtension;
import io.smallrye.reactive.messaging.impl.ConfiguredStreamFactory;
import io.smallrye.reactive.messaging.impl.StreamFactoryImpl;
import io.smallrye.reactive.messaging.impl.StreamRegistryImpl;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.After;
import org.junit.Test;
import org.reactivestreams.Subscriber;

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
  public void testABeanProducingMessagesSentToKafka() throws InterruptedException {
    Weld weld = new Weld();

    weld.addBeanClass(MediatorFactory.class);
    weld.addBeanClass(StreamRegistryImpl.class);
    weld.addBeanClass(StreamFactoryImpl.class);
    weld.addBeanClass(ConfiguredStreamFactory.class);
    weld.addExtension(new ReactiveMessagingExtension());

    weld.addBeanClass(MqttMessagingProvider.class);
    weld.addBeanClass(ProducingBean.class);

    weld.disableDiscovery();

    container = weld.initialize();

    CountDownLatch latch = new CountDownLatch(10);
    usage.consumeIntegers("sink", 10, 10, TimeUnit.SECONDS,
      null,
      v -> latch.countDown());

    assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
  }

}
