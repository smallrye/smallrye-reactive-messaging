package io.smallrye.reactive.messaging;

import io.reactivex.Flowable;
import io.smallrye.reactive.messaging.beans.*;
import org.apache.commons.lang3.NotImplementedException;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.weld.environment.se.WeldContainer;
import org.jboss.weld.exceptions.DefinitionException;
import org.junit.Test;
import org.reactivestreams.Subscriber;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class SubscriberShapeTest extends WeldTestBaseWithoutTails {


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

  @Test
  public void testThatWeCanConsumeMessagesFromAMethodReturningVoid() {
    // This case is not supported as it forces blocking acknowledgment.
    // See the MediatorConfiguration class for details.
    weld.addBeanClass(BeanConsumingMessagesAndReturningVoid.class);
    try {
      weld.initialize();
      fail("Expected failure - method validation should have failed");
    } catch (DefinitionException e) {
      // Check we have the right cause
      assertThat(e).hasMessageContaining("Invalid method").hasMessageContaining("acknowledgment");
    }
  }

  @Test
  public void testThatWeCanConsumePayloadsFromAMethodReturningVoid() {
    weld.addBeanClass(BeanConsumingPayloadsAndReturningVoid.class);
    WeldContainer container = weld.initialize();
    BeanConsumingPayloadsAndReturningVoid collector = container.getBeanManager()
      .createInstance().select(BeanConsumingPayloadsAndReturningVoid.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

  @Test
  public void testThatWeCanConsumeMessagesFromAMethodReturningSomething() {
    // This case is not supported as it forces blocking acknowledgment.
    // See the MediatorConfiguration class for details.

    weld.addBeanClass(BeanConsumingMessagesAndReturningSomething.class);
    try {
      weld.initialize();
      fail("Expected failure - method validation should have failed");
    } catch (DefinitionException e) {
      // Check we have the right cause
      assertThat(e).hasMessageContaining("Invalid method").hasMessageContaining("acknowledgment");
    }
  }

  @Test
  public void testThatWeCanConsumePayloadsFromAMethodReturningSomething() {
    weld.addBeanClass(BeanConsumingPayloadsAndReturningSomething.class);
    WeldContainer container = weld.initialize();
    BeanConsumingPayloadsAndReturningSomething collector = container.getBeanManager()
      .createInstance().select(BeanConsumingPayloadsAndReturningSomething.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

  @Test
  public void testThatWeCanConsumeMessagesFromAMethodReturningACompletionStage() {
    weld.addBeanClass(BeanConsumingMessagesAndReturningACompletionStageOfVoid.class);
    WeldContainer container = weld.initialize();
    BeanConsumingMessagesAndReturningACompletionStageOfVoid collector = container.getBeanManager()
      .createInstance().select(BeanConsumingMessagesAndReturningACompletionStageOfVoid.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

  @Test
  public void testThatWeCanConsumePayloadsFromAMethodReturningACompletionStage() {
    weld.addBeanClass(BeanConsumingPayloadsAndReturningACompletionStageOfVoid.class);
    WeldContainer container = weld.initialize();
    BeanConsumingPayloadsAndReturningACompletionStageOfVoid collector = container.getBeanManager()
      .createInstance().select(BeanConsumingPayloadsAndReturningACompletionStageOfVoid.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

  @Test
  public void testThatWeCanConsumeMessagesFromAMethodReturningACompletionStageOfSomething() {
    weld.addBeanClass(BeanConsumingMessagesAndReturningACompletionStageOfSomething.class);
    WeldContainer container = weld.initialize();
    BeanConsumingMessagesAndReturningACompletionStageOfSomething collector = container.getBeanManager()
      .createInstance().select(BeanConsumingMessagesAndReturningACompletionStageOfSomething.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

  @Test
  public void testThatWeCanConsumePayloadsFromAMethodReturningACompletionStageOfSomething() {
    weld.addBeanClass(BeanConsumingPayloadsAndReturningACompletionStageOfSomething.class);
    WeldContainer container = weld.initialize();
    BeanConsumingPayloadsAndReturningACompletionStageOfSomething collector = container.getBeanManager()
      .createInstance().select(BeanConsumingPayloadsAndReturningACompletionStageOfSomething.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
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
