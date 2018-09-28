package io.smallrye.reactive.messaging;

import java.util.List;

import javax.enterprise.inject.se.SeContainer;

import io.reactivex.Flowable;
import io.smallrye.reactive.messaging.beans.BeanProducingACompletableFuture;
import io.smallrye.reactive.messaging.beans.BeanProducingACompletableFutureOfMessage;
import io.smallrye.reactive.messaging.beans.BeanProducingACompletionStage;
import io.smallrye.reactive.messaging.beans.BeanProducingACompletionStageOfMessage;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ProcessorShapeReturningCompletionStagesTest extends WeldTestBase {

  private static final List<String> LIST =
    Flowable.range(1, 10).map(i -> Integer.toString(i)).toList().blockingGet();

  @Test
  public void testBeanProducingACompletionStageOfMessage() {
    addBeanClass(BeanProducingACompletionStageOfMessage.class);
    initialize();
    MyCollector collector = container.select(MyCollector.class).get();
    assertThat(collector.payloads()).isEqualTo(LIST);
  }

  @Test
  public void testBeanProducingACompletionStageOfPayloads() {
    addBeanClass(BeanProducingACompletionStage.class);
    initialize();
    MyCollector collector = container.select(MyCollector.class).get();
    assertThat(collector.payloads()).isEqualTo(LIST);
  }

  @Test
  public void testBeanProducingACompletableFutureOfMessage() {
    addBeanClass(BeanProducingACompletableFutureOfMessage.class);
    initialize();
    MyCollector collector = container.select(MyCollector.class).get();
    assertThat(collector.payloads()).isEqualTo(LIST);
  }

  @Test
  public void testBeanProducingACompletableFutureOfPayloads() {
    addBeanClass(BeanProducingACompletableFuture.class);
    initialize();
    MyCollector collector = container.select(MyCollector.class).get();
    assertThat(collector.payloads()).isEqualTo(LIST);
  }


}
