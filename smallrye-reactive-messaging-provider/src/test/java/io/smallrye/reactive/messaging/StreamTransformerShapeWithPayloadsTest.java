package io.smallrye.reactive.messaging;

import io.smallrye.reactive.messaging.beans.BeanConsumingItemAsFlowableAndPublishingItemAsFlowable;
import io.smallrye.reactive.messaging.beans.BeanConsumingItemAsFlowableAndPublishingItemAsPublisher;
import io.smallrye.reactive.messaging.beans.BeanConsumingItemAsPublisherAndPublishingItemAsFlowable;
import io.smallrye.reactive.messaging.beans.BeanConsumingItemAsPublisherBuilderAndPublishingItemAsPublisherBuilder;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class StreamTransformerShapeWithPayloadsTest extends WeldTestBase {

  @Test
  public void testBeanConsumingItemAsFlowableAndPublishingItemAsFlowable() {
    addBeanClass(BeanConsumingItemAsFlowableAndPublishingItemAsFlowable.class);
    initialize();
    MyCollector collector = container.getBeanManager().createInstance().select(MyCollector.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

  @Test
  public void testBeanConsumingItemAsFlowableAndPublishingItemAsPublisher() {
    addBeanClass(BeanConsumingItemAsFlowableAndPublishingItemAsPublisher.class);
    initialize();
    MyCollector collector = container.getBeanManager().createInstance().select(MyCollector.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

  @Test
  public void testBeanConsumingItemAsPublisherAndPublishingItemAsFlowable() {
    addBeanClass(BeanConsumingItemAsPublisherAndPublishingItemAsFlowable.class);
    initialize();
    MyCollector collector = container.getBeanManager().createInstance().select(MyCollector.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

  @Test
  public void testBeanConsumingItemAsPublisherBuilderAndPublishingItemAsPublisherBuilder() {
    addBeanClass(BeanConsumingItemAsPublisherBuilderAndPublishingItemAsPublisherBuilder.class);
    initialize();
    MyCollector collector = container.getBeanManager().createInstance().select(MyCollector.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

}
