package io.smallrye.reactive.messaging;

import io.reactivex.Flowable;
import io.smallrye.reactive.messaging.beans.BeanConsumingItemsAndProducingItems;
import io.smallrye.reactive.messaging.beans.BeanConsumingItemsAndProducingMessages;
import io.smallrye.reactive.messaging.beans.BeanConsumingMessagesAndProducingItems;
import io.smallrye.reactive.messaging.beans.BeanConsumingMessagesAndProducingMessages;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class ProcessorShapeConsumingIndividualItemsTest extends WeldTestBase {

  private static final List<String> LIST =
    Flowable.range(1, 10).map(i -> Integer.toString(i)).toList().blockingGet();

  @Test
  public void testBeanConsumingMessagesAndProducingMessages() {
    weld.addBeanClass(BeanConsumingMessagesAndProducingMessages.class);
    WeldContainer container = weld.initialize();
    MyCollector collector = container.getBeanManager().createInstance().select(MyCollector.class).get();
    assertThat(collector.payloads()).isEqualTo(LIST);
  }

  @Test
  public void testBeanConsumingMessagesAndProducingItems() {
    weld.addBeanClass(BeanConsumingMessagesAndProducingItems.class);
    WeldContainer container = weld.initialize();
    MyCollector collector = container.getBeanManager().createInstance().select(MyCollector.class).get();
    assertThat(collector.payloads()).isEqualTo(LIST);
  }

  @Test
  public void testBeanConsumingItemsAndProducingMessages() {
    weld.addBeanClass(BeanConsumingItemsAndProducingMessages.class);
    WeldContainer container = weld.initialize();
    MyCollector collector = container.getBeanManager().createInstance().select(MyCollector.class).get();
    assertThat(collector.payloads()).isEqualTo(LIST);
  }

  @Test
  public void testBeanConsumingItemsAndProducingItems() {
    weld.addBeanClass(BeanConsumingItemsAndProducingItems.class);
    WeldContainer container = weld.initialize();
    MyCollector collector = container.getBeanManager().createInstance().select(MyCollector.class).get();
    assertThat(collector.payloads()).isEqualTo(LIST);
  }

}
