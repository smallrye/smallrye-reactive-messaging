package io.smallrye.reactive.messaging;

import io.smallrye.reactive.messaging.beans.*;
import org.jboss.weld.environment.se.WeldContainer;
import org.jboss.weld.exceptions.DefinitionException;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class StreamTransformerShapeWithPayloadsTest extends WeldTestBase {

  @Test
  public void testBeanConsumingItemAsFlowableAndPublishingItemAsFlowable() {
    weld.addBeanClass(BeanConsumingItemAsFlowableAndPublishingItemAsFlowable.class);
    WeldContainer container = weld.initialize();
    MyCollector collector = container.getBeanManager().createInstance().select(MyCollector.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

  @Test
  public void testBeanConsumingItemAsFlowableAndPublishingItemAsPublisher() {
    weld.addBeanClass(BeanConsumingItemAsFlowableAndPublishingItemAsPublisher.class);
    WeldContainer container = weld.initialize();
    MyCollector collector = container.getBeanManager().createInstance().select(MyCollector.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

  @Test
  public void testBeanConsumingItemAsPublisherAndPublishingItemAsFlowable() {
    weld.addBeanClass(BeanConsumingItemAsPublisherAndPublishingItemAsFlowable.class);
    WeldContainer container = weld.initialize();
    MyCollector collector = container.getBeanManager().createInstance().select(MyCollector.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

  @Test
  public void testBeanConsumingItemAsPublisherBuilderAndPublishingItemAsPublisherBuilder() {
    weld.addBeanClass(BeanConsumingItemAsPublisherBuilderAndPublishingItemAsPublisherBuilder.class);
    WeldContainer container = weld.initialize();
    MyCollector collector = container.getBeanManager().createInstance().select(MyCollector.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

}
