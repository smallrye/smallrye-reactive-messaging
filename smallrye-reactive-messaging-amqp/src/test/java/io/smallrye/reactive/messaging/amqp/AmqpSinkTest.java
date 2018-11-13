package io.smallrye.reactive.messaging.amqp;

import io.reactivex.Flowable;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.reactivestreams.Subscriber;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.vertx.proton.ProtonHelper.message;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.core.Is.is;

@RunWith(Parameterized.class)
public class AmqpSinkTest extends AmqpTestBase {

  @Parameterized.Parameters
  public static Object[][] data() {
    return new Object[10][0];
  }

  private WeldContainer container;

  @After
  public void cleanup() {
    if (container != null) {
      container.close();
    }
  }

  @Test
  public void testSinkUsingInteger() {
    String topic = UUID.randomUUID().toString();
    AtomicInteger expected = new AtomicInteger(0);
    usage.consumeTenIntegers(topic,
      v -> expected.getAndIncrement());

    AmqpSink sink = getSink(topic);
    await().until(sink::isOpen);
    @SuppressWarnings("unchecked")
    Subscriber<Message> subscriber = (Subscriber<Message>) sink.subscriber();

    Flowable.range(0, 10)
      .map(v -> (Message) Message.of(v))
      .subscribe(subscriber);

    await().untilAtomic(expected, is(10));
    assertThat(expected).hasValue(10);
  }

  @Test
  public void testSinkUsingString() {
    String topic = UUID.randomUUID().toString();

    AmqpSink sink = getSink(topic);
    await().until(sink::isOpen);

    AtomicInteger expected = new AtomicInteger(0);
    usage.consumeTenStrings(topic,
      v -> expected.getAndIncrement());

    @SuppressWarnings("unchecked")
    Subscriber<Message> subscriber = (Subscriber<Message>) sink.subscriber();

    Flowable.range(0, 10)
      .map(i -> Integer.toString(i))
      .map(Message::of)
      .subscribe(subscriber);

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
    usage.consumeTenIntegers("sink",
      v -> latch.countDown());

    assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
  }


  @Test
  public void testSinkUsingAmqpMessage() {
    String topic = UUID.randomUUID().toString();
    AtomicInteger expected = new AtomicInteger(0);

    List<AmqpMessage<String>> messages = new ArrayList<>();
    usage.<String>consumeTenMessages(topic,
      v -> {
        expected.getAndIncrement();
        messages.add(v);
      });

    AmqpSink sink = getSink(topic);
    await().until(sink::isOpen);
    @SuppressWarnings("unchecked")
    Subscriber<Message> subscriber = (Subscriber<Message>) sink.subscriber();

    Flowable.range(0, 10)
      .map(v -> {
        AmqpMessage<String> message = new AmqpMessage<>("hello-" + v);
        message.unwrap().setSubject("foo");
        return message;
      })
      .subscribe(subscriber);

    await().untilAtomic(expected, is(10));
    assertThat(expected).hasValue(10);

    messages.forEach(m -> {
      assertThat(m.getPayload()).isInstanceOf(String.class).startsWith("hello-");
      assertThat(m.getSubject()).isEqualTo("foo");
      assertThat(m.delivery()).isNotNull();
    });
  }

  @Test
  public void testSinkUsingProtonMessage() {
    String topic = UUID.randomUUID().toString();
    AtomicInteger expected = new AtomicInteger(0);

    List<AmqpMessage<String>> messages = new ArrayList<>();
    usage.<String>consumeTenMessages(topic,
      v -> {
        expected.getAndIncrement();
        messages.add(v);
      });

    AmqpSink sink = getSink(topic);
    await().until(sink::isOpen);
    @SuppressWarnings("unchecked")
    Subscriber<Message> subscriber = (Subscriber<Message>) sink.subscriber();

    Flowable.range(0, 10)
      .map(v -> {
        org.apache.qpid.proton.message.Message message = message();
        message.setBody(new AmqpValue("hello-" + v));
        message.setSubject("bar");
        return message;
      })
      .map(Message::of)
      .subscribe(subscriber);

    await().untilAtomic(expected, is(10));
    assertThat(expected).hasValue(10);

    messages.forEach(m -> {
      assertThat(m.getPayload()).isInstanceOf(String.class).startsWith("hello-");
      assertThat(m.getSubject()).isEqualTo("bar");
      assertThat(m.delivery()).isNotNull();
    });
  }

  private AmqpSink getSink(String topic) {
    Map<String, String> config = new HashMap<>();
    config.put("address", topic);
    config.put("host", address);
    config.put("durable", "false");
    config.put("port", Integer.toString(port));
    config.put("username", "artemis");
    config.put("password", new String("simetraehcapa".getBytes()));

    AmqpMessagingProvider provider = new AmqpMessagingProvider(vertx);
    provider.configure();
    return provider.getSink(config).toCompletableFuture().join();
  }


}
