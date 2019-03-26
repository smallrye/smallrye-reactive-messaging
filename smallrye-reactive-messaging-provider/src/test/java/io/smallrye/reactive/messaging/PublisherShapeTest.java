package io.smallrye.reactive.messaging;

import io.reactivex.Flowable;
import io.reactivex.Single;
import io.smallrye.reactive.messaging.beans.BeanProducingMessagesAsFlowable;
import io.smallrye.reactive.messaging.beans.BeanProducingMessagesAsPublisher;
import io.smallrye.reactive.messaging.beans.BeanProducingMessagesAsPublisherBuilder;
import io.smallrye.reactive.messaging.beans.BeanProducingPayloadAsFlowable;
import io.smallrye.reactive.messaging.beans.BeanProducingPayloadAsPublisher;
import io.smallrye.reactive.messaging.beans.BeanProducingPayloadAsPublisherBuilder;
import io.smallrye.reactive.messaging.beans.BeanReturningAPublisherBuilderOfItems;
import io.smallrye.reactive.messaging.beans.BeanReturningAPublisherBuilderOfMessages;
import io.smallrye.reactive.messaging.beans.BeanReturningAPublisherOfItems;
import io.smallrye.reactive.messaging.beans.BeanReturningAPublisherOfMessages;
import io.smallrye.reactive.messaging.beans.BeanReturningCompletionStageOfMessage;
import io.smallrye.reactive.messaging.beans.BeanReturningCompletionStageOfPayload;
import io.smallrye.reactive.messaging.beans.BeanReturningMessages;
import io.smallrye.reactive.messaging.beans.BeanReturningPayloads;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.junit.Test;
import org.reactivestreams.Publisher;

import javax.enterprise.inject.se.SeContainer;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class PublisherShapeTest extends WeldTestBaseWithoutTails {


  @Override
  public List<Class> getBeans() {
    return Collections.singletonList(CollectorOnly.class);
  }

  @Test
  public void testBeanProducingMessagesAsFlowable() {
    addBeanClass(BeanProducingMessagesAsFlowable.class);
    initialize();
    CollectorOnly collector = container.select(CollectorOnly.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

  @Test
  public void testBeanProducingPayloadsAsFlowable() {
    addBeanClass(BeanProducingPayloadAsFlowable.class);
    initialize();
    CollectorOnly collector = container.select(CollectorOnly.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

  @Test
  public void testBeanProducingMessagesAsPublisher() {
    addBeanClass(BeanProducingMessagesAsPublisher.class);
    initialize();
    CollectorOnly collector = container.select(CollectorOnly.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

  @Test
  public void testBeanProducingPayloadsAsPublisher() {
    addBeanClass(BeanProducingPayloadAsPublisher.class);
    initialize();
    CollectorOnly collector = container.select(CollectorOnly.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

  @Test
  public void testBeanProducingMessagesAsPublisherBuilder() {
    addBeanClass(BeanProducingMessagesAsPublisherBuilder.class);
    initialize();
    CollectorOnly collector = container.select(CollectorOnly.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

  @Test
  public void testBeanProducingPayloadAsPublisherBuilder() {
    addBeanClass(BeanProducingPayloadAsPublisherBuilder.class);
    initialize();
    CollectorOnly collector = container.select(CollectorOnly.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

  @Test
  public void testThatWeCanProducePublisherOfMessages() {
    addBeanClass(BeanReturningAPublisherOfMessages.class);
    initialize();
    assertThatProducerWasPublished(container);
  }

  @Test
  public void testThatWeCanProducePublisherBuilderOfMessages() {
    addBeanClass(BeanReturningAPublisherBuilderOfMessages.class);
    initialize();
    assertThatProducerWasPublished(container);
  }

  @Test
  public void testThatWeCanProducePublisherOfItems() {
    addBeanClass(BeanReturningAPublisherOfItems.class);
    initialize();
    assertThatProducerWasPublished(container);
  }

  @Test
  public void testThatWeCanProducePublisherBuilderOfItems() {
    addBeanClass(BeanReturningAPublisherBuilderOfItems.class);
    initialize();
    assertThatProducerWasPublished(container);
  }

  @Test
  public void testThatWeCanProducePayloadDirectly() {
    addBeanClass(BeanReturningPayloads.class);
    initialize();

    List<PublisherBuilder<? extends Message>> producer = registry(container).getPublishers("infinite-producer");
    assertThat(producer).isNotEmpty();
    List<Object> list = producer.get(0).map(Message::getPayload)
      .limit(3).toList().run().toCompletableFuture().join();
    assertThat(list).containsExactly(1, 2, 3);
  }

  @Test
  public void testThatWeCanProduceMessageDirectly() {
    addBeanClass(BeanReturningMessages.class);
    initialize();

    List<PublisherBuilder<? extends Message>> producer = registry(container).getPublishers("infinite-producer");
    assertThat(producer).isNotEmpty();
    List<Object> list = producer.get(0).map(Message::getPayload)
      .limit(5).toList().run().toCompletableFuture().join();
    assertThat(list).containsExactly(1, 2, 3, 4, 5);
  }

  @Test
  public void testThatWeCanProduceCompletionStageOfMessageDirectly() {
    addBeanClass(BeanReturningCompletionStageOfMessage.class);
    initialize();

    List<PublisherBuilder<? extends Message>> producer = registry(container).getPublishers("infinite-producer");
    assertThat(producer).isNotEmpty();
    List<Object> list = producer.get(0).map(Message::getPayload)
      .limit(10).toList().run().toCompletableFuture().join();
    assertThat(list).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
  }

  @Test
  public void testThatWeCanProduceCompletionStageOfPayloadDirectly() {
    addBeanClass(BeanReturningCompletionStageOfPayload.class);
    initialize();

    List<PublisherBuilder<? extends Message>> producer = registry(container).getPublishers("infinite-producer");
    assertThat(producer).isNotEmpty();
    List<Object> list = producer.get(0).map(Message::getPayload)
      .limit(4).toList().run().toCompletableFuture().join();
    assertThat(list).containsExactly(1, 2, 3, 4);
  }

  private void assertThatProducerWasPublished(SeContainer container) {
    assertThat(registry(container).getIncomingNames()).contains("producer");
    List<PublisherBuilder<? extends Message>> producer = registry(container).getPublishers("producer");
    assertThat(producer).isNotEmpty();
    List<Object> list = producer.get(0).map(Message::getPayload).toList().run().toCompletableFuture().join();
    assertThat(list).containsExactly("a", "b", "c");
  }

}
