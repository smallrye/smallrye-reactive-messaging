package io.smallrye.reactive.messaging;

import io.reactivex.Flowable;
import io.smallrye.reactive.messaging.beans.*;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class ProducerTest extends WeldTestBase {

  @Test
  public void testThatWeCanProducePublisher() {
    weld.addBeanClass(BeanProducingAPublisher.class);
    WeldContainer container = weld.initialize();
    assertThatProducerWasPublished(container);
  }

  private void assertThatProducerWasPublished(WeldContainer container) {
    assertThat(registry(container).getPublisherNames()).contains("producer");
    Optional<Publisher<? extends Message>> producer = registry(container).getPublisher("producer");
    assertThat(producer).isNotEmpty()
      .flatMap(publisher -> Flowable.fromPublisher(publisher).map(Message::getPayload).toList().to(s -> Optional.of(s.blockingGet())))
      .contains(Arrays.asList("a", "b", "c"));
  }

  @Test
  public void testThatWeCanProduceSubscriber() {
    weld.addBeanClass(BeanProducingASubscriber.class);
    WeldContainer container = weld.initialize();
    assertThatSubscriberWasPublished(container);
  }

  @SuppressWarnings("unchecked")
  private void assertThatSubscriberWasPublished(WeldContainer container) {
    assertThat(registry(container).getSubscriberNames()).contains("subscriber");
    Optional<Subscriber<? extends Message>> subscriber = registry(container).getSubscriber("subscriber");
    assertThat(subscriber).isNotEmpty();
    List<String> list = new ArrayList<>();
    Flowable.just("a", "b", "c").map(DefaultMessage::create)
      .doOnNext(m -> list.add(m.getPayload()))
      .subscribe(((Subscriber<Message>) subscriber.orElseThrow(() -> new AssertionError("Subscriber should be present"))));
    assertThat(list).containsExactly("a", "b", "c");
  }

  @Test
  public void testThatWeCanProduceFlowable() {
    weld.addBeanClass(BeanProducingAFlowable.class);
    WeldContainer container = weld.initialize();
    assertThatProducerWasPublished(container);
  }

  @Test
  public void testThatWeCanProducePublisherBuilder() {
    weld.addBeanClass(BeanProducingAPublisherBuilder.class);
    WeldContainer container = weld.initialize();
    assertThatProducerWasPublished(container);
  }

  @Test
  public void testThatWeCanProduceSubscriberBuilder() {
    weld.addBeanClass(BeanProducingASubscriberBuilder.class);
    WeldContainer container = weld.initialize();
    assertThatSubscriberWasPublished(container);
  }
}
