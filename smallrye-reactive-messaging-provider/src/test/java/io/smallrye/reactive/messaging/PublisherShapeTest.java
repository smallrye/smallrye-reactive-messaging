package io.smallrye.reactive.messaging;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import javax.enterprise.inject.se.SeContainer;

import io.reactivex.Flowable;
import io.smallrye.reactive.messaging.beans.*;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.ReactiveStreams;
import org.junit.Test;
import org.reactivestreams.Publisher;

import static org.assertj.core.api.Assertions.assertThat;

public class PublisherShapeTest extends WeldTestBaseWithoutTails {


  @Override
  public List<Class> getBeans() {
    return Collections.singletonList(CollectorOnly.class);
  }

  @Test
  public void testBeanProducingMessagesAsFlowable() {
    initializer.addBeanClasses(BeanProducingMessagesAsFlowable.class);
    SeContainer container = initializer.initialize();
    CollectorOnly collector = container.select(CollectorOnly.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

  @Test
  public void testBeanProducingPayloadsAsFlowable() {
    initializer.addBeanClasses(BeanProducingPayloadAsFlowable.class);
    SeContainer container = initializer.initialize();
    CollectorOnly collector = container.select(CollectorOnly.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

  @Test
  public void testBeanProducingMessagesAsPublisher() {
    initializer.addBeanClasses(BeanProducingMessagesAsPublisher.class);
    SeContainer container = initializer.initialize();
    CollectorOnly collector = container.select(CollectorOnly.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

  @Test
  public void testBeanProducingPayloadsAsPublisher() {
    initializer.addBeanClasses(BeanProducingPayloadAsPublisher.class);
    SeContainer container = initializer.initialize();
    CollectorOnly collector = container.select(CollectorOnly.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

  @Test
  public void testBeanProducingMessagesAsPublisherBuilder() {
    initializer.addBeanClasses(BeanProducingMessagesAsPublisherBuilder.class);
    SeContainer container = initializer.initialize();
    CollectorOnly collector = container.select(CollectorOnly.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

  @Test
  public void testBeanProducingPayloadAsPublisherBuilder() {
    initializer.addBeanClasses(BeanProducingPayloadAsPublisherBuilder.class);
    SeContainer container = initializer.initialize();
    CollectorOnly collector = container.select(CollectorOnly.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

  @Test
  public void testThatWeCanProducePublisherOfMessages() {
    initializer.addBeanClasses(BeanReturningAPublisherOfMessages.class);
    SeContainer container = initializer.initialize();
    assertThatProducerWasPublished(container);
  }

  @Test
  public void testThatWeCanProducePublisherBuilderOfMessages() {
    initializer.addBeanClasses(BeanReturningAPublisherBuilderOfMessages.class);
    SeContainer container = initializer.initialize();
    assertThatProducerWasPublished(container);
  }

  @Test
  public void testThatWeCanProducePublisherOfItems() {
    initializer.addBeanClasses(BeanReturningAPublisherOfItems.class);
    SeContainer container = initializer.initialize();
    assertThatProducerWasPublished(container);
  }

  @Test
  public void testThatWeCanProducePublisherBuilderOfItems() {
    initializer.addBeanClasses(BeanReturningAPublisherBuilderOfItems.class);
    SeContainer container = initializer.initialize();
    assertThatProducerWasPublished(container);
  }

  @Test
  public void testThatWeCanProducePayloadDirectly() {
    initializer.addBeanClasses(BeanReturningPayloads.class);
    SeContainer container = initializer.initialize();

    Optional<Publisher<? extends Message>> producer = registry(container).getPublisher("infinite-producer");
    assertThat(producer).isNotEmpty();
    List<Object> list = ReactiveStreams.fromPublisher(producer.get()).map(Message::getPayload)
      .limit(3).toList().run().toCompletableFuture().join();
    assertThat(list).containsExactly(1, 2, 3);
  }

  @Test
  public void testThatWeCanProduceMessageDirectly() {
    initializer.addBeanClasses(BeanReturningMessages.class);
    SeContainer container = initializer.initialize();

    Optional<Publisher<? extends Message>> producer = registry(container).getPublisher("infinite-producer");
    assertThat(producer).isNotEmpty();
    List<Object> list = ReactiveStreams.fromPublisher(producer.get()).map(Message::getPayload)
      .limit(5).toList().run().toCompletableFuture().join();
    assertThat(list).containsExactly(1, 2, 3, 4, 5);
  }

  @Test
  public void testThatWeCanProduceCompletionStageOfMessageDirectly() {
    initializer.addBeanClasses(BeanReturningCompletionStageOfMessage.class);
    SeContainer container = initializer.initialize();

    Optional<Publisher<? extends Message>> producer = registry(container).getPublisher("infinite-producer");
    assertThat(producer).isNotEmpty();
    List<Object> list = ReactiveStreams.fromPublisher(producer.get()).map(Message::getPayload)
      .limit(10).toList().run().toCompletableFuture().join();
    assertThat(list).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
  }

  @Test
  public void testThatWeCanProduceCompletionStageOfPayloadDirectly() {
    initializer.addBeanClasses(BeanReturningCompletionStageOfPayload.class);
    SeContainer container = initializer.initialize();

    Optional<Publisher<? extends Message>> producer = registry(container).getPublisher("infinite-producer");
    assertThat(producer).isNotEmpty();
    List<Object> list = ReactiveStreams.fromPublisher(producer.get()).map(Message::getPayload)
      .limit(4).toList().run().toCompletableFuture().join();
    assertThat(list).containsExactly(1, 2, 3, 4);
  }

  private void assertThatProducerWasPublished(SeContainer container) {
    assertThat(registry(container).getPublisherNames()).contains("producer");
    Optional<Publisher<? extends Message>> producer = registry(container).getPublisher("producer");
    assertThat(producer).isNotEmpty()
      .flatMap(publisher -> Flowable.fromPublisher(publisher).map(Message::getPayload).toList().to(s -> Optional.of(s.blockingGet())))
      .contains(Arrays.asList("a", "b", "c"));
  }






}
