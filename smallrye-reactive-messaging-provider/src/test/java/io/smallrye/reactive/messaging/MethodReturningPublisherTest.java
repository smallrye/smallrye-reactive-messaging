package io.smallrye.reactive.messaging;

import io.reactivex.Flowable;
import io.smallrye.reactive.messaging.beans.*;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.Test;
import org.reactivestreams.Publisher;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class MethodReturningPublisherTest extends WeldTestBaseWithoutTails {


  @Override
  public List<Class> getBeans() {
    return Collections.singletonList(CollectorOnly.class);
  }

  @Test
  public void testBeanProducingMessagesAsFlowable() {
    weld.addBeanClass(BeanProducingMessagesAsFlowable.class);
    WeldContainer container = weld.initialize();
    CollectorOnly collector = container.getBeanManager().createInstance().select(CollectorOnly.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

  @Test
  public void testBeanProducingPayloadsAsFlowable() {
    weld.addBeanClass(BeanProducingPayloadAsFlowable.class);
    WeldContainer container = weld.initialize();
    CollectorOnly collector = container.getBeanManager().createInstance().select(CollectorOnly.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

  @Test
  public void testBeanProducingMessagesAsPublisher() {
    weld.addBeanClass(BeanProducingMessagesAsPublisher.class);
    WeldContainer container = weld.initialize();
    CollectorOnly collector = container.getBeanManager().createInstance().select(CollectorOnly.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

  @Test
  public void testBeanProducingPayloadsAsPublisher() {
    weld.addBeanClass(BeanProducingPayloadAsPublisher.class);
    WeldContainer container = weld.initialize();
    CollectorOnly collector = container.getBeanManager().createInstance().select(CollectorOnly.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

  @Test
  public void testBeanProducingMessagesAsPublisherBuilder() {
    weld.addBeanClass(BeanProducingMessagesAsPublisherBuilder.class);
    WeldContainer container = weld.initialize();
    CollectorOnly collector = container.getBeanManager().createInstance().select(CollectorOnly.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

  @Test
  public void testBeanProducingPayloadAsPublisherBuilder() {
    weld.addBeanClass(BeanProducingPayloadAsPublisherBuilder.class);
    WeldContainer container = weld.initialize();
    CollectorOnly collector = container.getBeanManager().createInstance().select(CollectorOnly.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

  @Test
  public void testThatWeCanProducePublisherOfMessages() {
    weld.addBeanClass(BeanReturningAPublisherOfMessages.class);
    WeldContainer container = weld.initialize();
    assertThatProducerWasPublished(container);
  }

  @Test
  public void testThatWeCanProducePublisherBuilderOfMessages() {
    weld.addBeanClass(BeanReturningAPublisherBuilderOfMessages.class);
    WeldContainer container = weld.initialize();
    assertThatProducerWasPublished(container);
  }

  @Test
  public void testThatWeCanProducePublisherOfItems() {
    weld.addBeanClass(BeanReturningAPublisherOfItems.class);
    WeldContainer container = weld.initialize();
    assertThatProducerWasPublished(container);
  }

  @Test
  public void testThatWeCanProducePublisherBuilderOfItems() {
    weld.addBeanClass(BeanReturningAPublisherBuilderOfItems.class);
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






}
