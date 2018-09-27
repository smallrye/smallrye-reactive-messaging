package io.smallrye.reactive.messaging;

import java.util.List;

import javax.enterprise.inject.se.SeContainer;

import io.reactivex.Flowable;
import io.smallrye.reactive.messaging.beans.BeanConsumingItemsAndProducingItems;
import io.smallrye.reactive.messaging.beans.BeanConsumingItemsAndProducingMessages;
import io.smallrye.reactive.messaging.beans.BeanConsumingMessagesAndProducingItems;
import io.smallrye.reactive.messaging.beans.BeanConsumingMessagesAndProducingMessages;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ProcessorShapeConsumingIndividualItemsTest extends WeldTestBase {

  private static final List<String> LIST =
    Flowable.range(1, 10).map(i -> Integer.toString(i)).toList().blockingGet();

  @Test
  public void testBeanConsumingMessagesAndProducingMessages() {
    initializer.addBeanClasses(BeanConsumingMessagesAndProducingMessages.class);
    SeContainer container = initializer.initialize();
    MyCollector collector = container.select(MyCollector.class).get();
    assertThat(collector.payloads()).isEqualTo(LIST);
  }

  @Test
  public void testBeanConsumingMessagesAndProducingItems() {
    initializer.addBeanClasses(BeanConsumingMessagesAndProducingItems.class);
    SeContainer container = initializer.initialize();
    MyCollector collector = container.select(MyCollector.class).get();
    assertThat(collector.payloads()).isEqualTo(LIST);
  }

  @Test
  public void testBeanConsumingItemsAndProducingMessages() {
    initializer.addBeanClasses(BeanConsumingItemsAndProducingMessages.class);
    SeContainer container = initializer.initialize();
    MyCollector collector = container.select(MyCollector.class).get();
    assertThat(collector.payloads()).isEqualTo(LIST);
  }

  @Test
  public void testBeanConsumingItemsAndProducingItems() {
    initializer.addBeanClasses(BeanConsumingItemsAndProducingItems.class);
    SeContainer container = initializer.initialize();
    MyCollector collector = container.select(MyCollector.class).get();
    assertThat(collector.payloads()).isEqualTo(LIST);
  }

}
