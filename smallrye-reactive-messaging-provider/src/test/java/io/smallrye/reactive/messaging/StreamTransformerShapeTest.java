package io.smallrye.reactive.messaging;

import io.smallrye.reactive.messaging.beans.*;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class StreamTransformerShapeTest extends WeldTestBase {

  @Test
  public void testBeanConsumingMsgAsFlowableAndPublishingMsgAsFlowable() {
    weld.addBeanClass(BeanConsumingMsgAsFlowableAndPublishingMsgAsFlowable.class);
    WeldContainer container = weld.initialize();
    MyCollector collector = container.getBeanManager().createInstance().select(MyCollector.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

  @Test
  public void testBeanConsumingMsgAsFlowableAndPublishingMsgAsPublisher() {
    weld.addBeanClass(BeanConsumingMsgAsFlowableAndPublishingMsgAsPublisher.class);
    WeldContainer container = weld.initialize();
    MyCollector collector = container.getBeanManager().createInstance().select(MyCollector.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

  @Test
  public void testBeanConsumingMsgAsPublisherAndPublishingMsgAsFlowable() {
    weld.addBeanClass(BeanConsumingMsgAsPublisherAndPublishingMsgAsFlowable.class);
    WeldContainer container = weld.initialize();
    MyCollector collector = container.getBeanManager().createInstance().select(MyCollector.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

  @Test
  public void testBeanConsumingMsgAsPublisherBuilderAndPublishingMsgAsPublisherBuilder() {
    weld.addBeanClass(BeanConsumingMsgAsPublisherBuilderAndPublishingMsgAsPublisherBuilder.class);
    WeldContainer container = weld.initialize();
    MyCollector collector = container.getBeanManager().createInstance().select(MyCollector.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

  @Test
  public void testBeanProducingAProcessor() {
    weld.addBeanClass(BeanProducingAProcessorOfMessages.class);
    WeldContainer container = weld.initialize();
    MyCollector collector = container.getBeanManager().createInstance().select(MyCollector.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

  @Test
  public void testBeanProducingAProcessorBuilder() {
    weld.addBeanClass(BeanProducingAProcessorBuilderOfMessages.class);
    WeldContainer container = weld.initialize();
    MyCollector collector = container.getBeanManager().createInstance().select(MyCollector.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }


}
