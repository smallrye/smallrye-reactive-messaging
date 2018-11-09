package io.smallrye.reactive.messaging.eventbus;

import io.reactivex.Flowable;
import io.smallrye.reactive.messaging.MediatorFactory;
import io.smallrye.reactive.messaging.ReactiveMessagingExtension;
import io.smallrye.reactive.messaging.impl.ConfiguredStreamFactory;
import io.smallrye.reactive.messaging.impl.InternalStreamRegistry;
import io.smallrye.reactive.messaging.impl.StreamFactoryImpl;
import io.smallrye.reactive.messaging.spi.ConfigurationHelper;
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

public class EventBusSinkTest extends EventbusTestBase {

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
    usage.consumeIntegers(topic, 10, 10, TimeUnit.SECONDS,
      v -> expected.getAndIncrement());


    Map<String, String> config = new HashMap<>();
    config.put("address", topic);
    EventBusSink sink = new EventBusSink(vertx, ConfigurationHelper.create(config));

    Subscriber subscriber = sink.subscriber();
    Flowable.range(0, 10)
      .map(v -> (Message) Message.of(v))
      .subscribe(subscriber);

    await().untilAtomic(expected, is(10));
    assertThat(expected).hasValue(10);
  }

  @Test
  public void testSinkUsingString() {
    String topic = UUID.randomUUID().toString();
    AtomicInteger expected = new AtomicInteger(0);
    usage.consumeStrings(topic, 10, 10, TimeUnit.SECONDS,
      v -> expected.getAndIncrement());

    Map<String, String> config = new HashMap<>();
    config.put("address", topic);
    EventBusSink sink = new EventBusSink(vertx, ConfigurationHelper.create(config));

    Subscriber subscriber = sink.subscriber();
    Flowable.range(0, 10)
      .map(i -> Integer.toString(i))
      .map(Message::of)
      .subscribe(subscriber);
    await().untilAtomic(expected, is(10));
    assertThat(expected).hasValue(10);
  }

  @Test
  public void testABeanProducingMessagesSentToEventBus() {
    Weld weld = new Weld();

    weld.addBeanClass(MediatorFactory.class);
    weld.addBeanClass(InternalStreamRegistry.class);
    weld.addBeanClass(StreamFactoryImpl.class);
    weld.addBeanClass(ConfiguredStreamFactory.class);
    weld.addExtension(new ReactiveMessagingExtension());

    weld.addBeanClass(VertxEventBusMessagingProvider.class);
    weld.addBeanClass(ProducingBean.class);

    weld.disableDiscovery();

    container = weld.initialize();
    ProducingBean bean = container.getBeanManager().createInstance().select(ProducingBean.class).get();

    await().until(() -> bean.messages().size() == 10);
  }

}
