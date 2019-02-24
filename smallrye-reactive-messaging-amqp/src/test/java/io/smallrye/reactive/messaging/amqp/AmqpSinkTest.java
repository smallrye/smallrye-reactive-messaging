package io.smallrye.reactive.messaging.amqp;

import io.reactivex.Flowable;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.After;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import repeat.Repeat;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.vertx.proton.ProtonHelper.message;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.core.Is.is;

public class AmqpSinkTest extends AmqpTestBase {

  public static final String HELLO = "hello-";
  private WeldContainer container;
  private AmqpMessagingProvider provider;

  @After
  public void cleanup() {
    if (provider != null) {
      provider.close();
    }

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

    SubscriberBuilder<? extends Message, Void> sink = createProviderAndSink(topic);
    //noinspection unchecked
    Flowable.range(0, 10)
      .map(v -> (Message) Message.of(v))
      .subscribe((Subscriber) sink.build());

    await().until(() -> expected.get() == 10);
    assertThat(expected).hasValue(10);
  }

  @Test
  public void testSinkUsingString() {
    String topic = UUID.randomUUID().toString();

    SubscriberBuilder<? extends Message, Void> sink = createProviderAndSink(topic);

    AtomicInteger expected = new AtomicInteger(0);
    usage.consumeTenStrings(topic,
      v -> expected.getAndIncrement());

    //noinspection unchecked
    Flowable.range(0, 10)
      .map(Integer::valueOf)
      .map(Message::of)
      .subscribe((Subscriber) sink.build());

    await().untilAtomic(expected, is(10));
    assertThat(expected).hasValue(10);
  }

  @Test
  @Repeat(times = 3)
  public void testABeanProducingMessagesSentToAMQP() throws InterruptedException {
    Weld weld = new Weld();

    CountDownLatch latch = new CountDownLatch(10);
    usage.consumeTenIntegers("sink",
      v -> latch.countDown());

    weld.addBeanClass(AmqpMessagingProvider.class);
    weld.addBeanClass(ProducingBean.class);

    container = weld.initialize();

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

    SubscriberBuilder<? extends Message, Void> sink = createProviderAndSink(topic);

    //noinspection unchecked
    Flowable.range(0, 10)
      .map(v -> {
        AmqpMessage<String> message = new AmqpMessage<>(HELLO + v);
        message.unwrap().setSubject("foo");
        return message;
      })
      .subscribe((Subscriber) sink.build());

    await().untilAtomic(expected, is(10));
    assertThat(expected).hasValue(10);

    messages.forEach(m -> {
      assertThat(m.getPayload()).isInstanceOf(String.class).startsWith(HELLO);
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

    SubscriberBuilder<? extends Message, Void> sink = createProviderAndSink(topic);

    //noinspection unchecked
    Flowable.range(0, 10)
      .map(v -> {
        org.apache.qpid.proton.message.Message message = message();
        message.setBody(new AmqpValue(HELLO + v));
        message.setSubject("bar");
        return message;
      })
      .map(Message::of)
      .subscribe((Subscriber) sink.build());

    await().untilAtomic(expected, is(10));
    assertThat(expected).hasValue(10);

    messages.forEach(m -> {
      assertThat(m.getPayload()).isInstanceOf(String.class).startsWith(HELLO);
      assertThat(m.getSubject()).isEqualTo("bar");
      assertThat(m.delivery()).isNotNull();
    });
  }

  private SubscriberBuilder<? extends Message, Void> createProviderAndSink(String topic) {
    Map<String, Object> config = new HashMap<>();
    config.put("address", topic);
    config.put("name", "the name");
    config.put("host", address);
    config.put("durable", false);
    config.put("port", port);
    config.put("username", "artemis");
    config.put("password", new String("simetraehcapa".getBytes()));

    this.provider = new AmqpMessagingProvider(vertx);
    return this.provider.getSubscriberBuilder(new MapBasedConfig(config));
  }

}
