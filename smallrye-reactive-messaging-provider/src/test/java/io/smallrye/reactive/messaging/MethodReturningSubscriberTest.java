package io.smallrye.reactive.messaging;

import io.reactivex.Flowable;
import io.smallrye.reactive.messaging.beans.*;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.Test;
import org.reactivestreams.Subscriber;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class MethodReturningSubscriberTest extends WeldTestBaseWithoutTails {


  @Override
  public List<Class> getBeans() {
    return Collections.singletonList(SourceOnly.class);
  }

  @Test
  public void testBeanProducingASubscriberOfMessages() {
    weld.addBeanClass(BeanReturningASubscriberOfMessages.class);
    WeldContainer container = weld.initialize();
    BeanReturningASubscriberOfMessages collector = container.getBeanManager().createInstance().select(BeanReturningASubscriberOfMessages.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

  @Test
  public void testBeanProducingASubscriberOfPayloads() {
    weld.addBeanClass(BeanReturningASubscriberOfPayloads.class);
    WeldContainer container = weld.initialize();
    BeanReturningASubscriberOfPayloads collector = container.getBeanManager().createInstance().select(BeanReturningASubscriberOfPayloads.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

  @Test
  public void testThatWeCanProduceSubscriberOfMessage() {
    weld.addBeanClass(BeanReturningASubscriberOfMessagesButDiscarding.class);
    WeldContainer container = weld.initialize();
    assertThatSubscriberWasPublished(container);
  }

  @SuppressWarnings("unchecked")
  private void assertThatSubscriberWasPublished(WeldContainer container) {
    assertThat(registry(container).getSubscriberNames()).contains("subscriber");
    Optional<Subscriber<? extends Message>> subscriber = registry(container).getSubscriber("subscriber");
    assertThat(subscriber).isNotEmpty();
    List<String> list = new ArrayList<>();
    Flowable.just("a", "b", "c").map(Message::of)
      .doOnNext(m -> list.add(m.getPayload()))
      .subscribe(((Subscriber<Message>) subscriber.orElseThrow(() -> new AssertionError("Subscriber should be present"))));
    assertThat(list).containsExactly("a", "b", "c");
  }


}
