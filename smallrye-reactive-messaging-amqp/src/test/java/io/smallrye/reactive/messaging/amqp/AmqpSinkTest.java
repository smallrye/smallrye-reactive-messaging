package io.smallrye.reactive.messaging.amqp;

import io.reactivex.Flowable;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.After;
import org.junit.Test;
import org.reactivestreams.Subscriber;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.vertx.proton.ProtonHelper.message;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.core.Is.is;

public class AmqpSinkTest extends AmqpTestBase {

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

    Subscriber subscriber = getConfiguredSubscriber(topic);

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
    config.put("address", topic);
    config.put("host", address);
    config.put("port", Integer.toString(port));
    config.put("username", "artemis");
    config.put("durable", "false");
    config.put("password", new String("simetraehcapa".getBytes()));

    AmqpMessagingProvider provider = new AmqpMessagingProvider(vertx);
    provider.configure();
    Subscriber subscriber = provider.createSubscriber(config).toCompletableFuture().join();

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

    weld.addBeanClass(AmqpMessagingProvider.class);
    weld.addBeanClass(ProducingBean.class);

    container = weld.initialize();

    CountDownLatch latch = new CountDownLatch(10);
    usage.consumeIntegers("sink", 10, 10, TimeUnit.SECONDS,
      null,
      v -> latch.countDown());

    assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
  }


  @Test
  public void testSinkUsingAmqpMessage() throws InterruptedException {
    String topic = UUID.randomUUID().toString();
    CountDownLatch latch = new CountDownLatch(1);
    AtomicInteger expected = new AtomicInteger(0);

    List<AmqpMessage<String>> messages = new ArrayList<>();
    usage.consumeMessages(topic, 10, 10, TimeUnit.SECONDS,
      latch::countDown,
      v -> {
        expected.getAndIncrement();
        messages.add(v);
      });

    Subscriber subscriber = getConfiguredSubscriber(topic);

    Flowable.range(0, 10)
      .map(v -> {
        AmqpMessage<String> message = new AmqpMessage<>("hello-" + v);
        message.unwrap().setSubject("foo");
        return message;
      })
      .subscribe(subscriber);

    assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
    await().untilAtomic(expected, is(10));
    assertThat(expected).hasValue(10);

    messages.forEach(m -> {
      assertThat(m.getPayload()).isInstanceOf(String.class).startsWith("hello-");
      assertThat(m.getSubject()).isEqualTo("foo");
      assertThat(m.delivery()).isNotNull();
    });
  }

  @Test
  public void testSinkUsingProtonMessage() throws InterruptedException {
    String topic = UUID.randomUUID().toString();
    CountDownLatch latch = new CountDownLatch(1);
    AtomicInteger expected = new AtomicInteger(0);

    List<AmqpMessage<String>> messages = new ArrayList<>();
    usage.consumeMessages(topic, 10, 10, TimeUnit.SECONDS,
      latch::countDown,
      v -> {
        expected.getAndIncrement();
        messages.add(v);
      });

    Subscriber subscriber = getConfiguredSubscriber(topic);

    Flowable.range(0, 10)
      .map(v -> {
        org.apache.qpid.proton.message.Message message = message();
        message.setBody(new AmqpValue("hello-" + v));
        message.setSubject("bar");
        return message;
      })
      .map(Message::of)
      .subscribe(subscriber);

    assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
    await().untilAtomic(expected, is(10));
    assertThat(expected).hasValue(10);

    messages.forEach(m -> {
      assertThat(m.getPayload()).isInstanceOf(String.class).startsWith("hello-");
      assertThat(m.getSubject()).isEqualTo("bar");
      assertThat(m.delivery()).isNotNull();
    });
  }

  private Subscriber getConfiguredSubscriber(String topic) {
    Map<String, String> config = new HashMap<>();
    config.put("address", topic);
    config.put("host", address);
    config.put("durable", "false");
    config.put("port", Integer.toString(port));
    config.put("username", "artemis");
    config.put("password", new String("simetraehcapa".getBytes()));

    AmqpMessagingProvider provider = new AmqpMessagingProvider(vertx);
    provider.configure();
    return provider.createSubscriber(config).toCompletableFuture().join();
  }

}
