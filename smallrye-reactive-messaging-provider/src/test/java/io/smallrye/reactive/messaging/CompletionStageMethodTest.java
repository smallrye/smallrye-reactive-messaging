package io.smallrye.reactive.messaging;

import io.reactivex.Flowable;
import io.smallrye.reactive.messaging.beans.*;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class CompletionStageMethodTest extends WeldTestBase {

  private static final List<String> LIST =
    Flowable.range(1, 10).map(i -> Integer.toString(i)).toList().blockingGet();

  @Test
  public void testBeanProducingACompletionStageOfMessage() {
    weld.addBeanClass(BeanProducingACompletionStageOfMessage.class);
    WeldContainer container = weld.initialize();
    MyCollector collector = container.getBeanManager().createInstance().select(MyCollector.class).get();
    assertThat(collector.payloads()).isEqualTo(LIST);
  }

  @Test
  public void testBeanProducingACompletionStageOfPayloads() {
    weld.addBeanClass(BeanProducingACompletionStage.class);
    WeldContainer container = weld.initialize();
    MyCollector collector = container.getBeanManager().createInstance().select(MyCollector.class).get();
    assertThat(collector.payloads()).isEqualTo(LIST);
  }

  @Test
  public void testBeanProducingACompletableFutureOfMessage() {
    weld.addBeanClass(BeanProducingACompletableFutureOfMessage.class);
    WeldContainer container = weld.initialize();
    MyCollector collector = container.getBeanManager().createInstance().select(MyCollector.class).get();
    assertThat(collector.payloads()).isEqualTo(LIST);
  }

  @Test
  public void testBeanProducingACompletableFutureOfPayloads() {
    weld.addBeanClass(BeanProducingACompletableFuture.class);
    WeldContainer container = weld.initialize();
    MyCollector collector = container.getBeanManager().createInstance().select(MyCollector.class).get();
    assertThat(collector.payloads()).isEqualTo(LIST);
  }


}
