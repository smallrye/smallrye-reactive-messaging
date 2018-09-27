package io.smallrye.reactive.messaging;

import javax.enterprise.inject.se.SeContainer;

import io.smallrye.reactive.messaging.beans.*;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class StreamTransformerShapeTest extends WeldTestBase {

  @Test
  public void testBeanConsumingMsgAsFlowableAndPublishingMsgAsFlowable() {
    initializer.addBeanClasses(BeanConsumingMsgAsFlowableAndPublishingMsgAsFlowable.class);
    SeContainer container = initializer.initialize();
    MyCollector collector = container.select(MyCollector.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

  @Test
  public void testBeanConsumingMsgAsFlowableAndPublishingMsgAsPublisher() {
    initializer.addBeanClasses(BeanConsumingMsgAsFlowableAndPublishingMsgAsPublisher.class);
    SeContainer container = initializer.initialize();
    MyCollector collector = container.select(MyCollector.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

  @Test
  public void testBeanConsumingMsgAsPublisherAndPublishingMsgAsFlowable() {
    initializer.addBeanClasses(BeanConsumingMsgAsPublisherAndPublishingMsgAsFlowable.class);
    SeContainer container = initializer.initialize();
    MyCollector collector = container.select(MyCollector.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

  @Test
  public void testBeanConsumingMsgAsPublisherBuilderAndPublishingMsgAsPublisherBuilder() {
    initializer.addBeanClasses(BeanConsumingMsgAsPublisherBuilderAndPublishingMsgAsPublisherBuilder.class);
    SeContainer container = initializer.initialize();
    MyCollector collector = container.select(MyCollector.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

  @Test
  public void testBeanProducingAProcessor() {
    initializer.addBeanClasses(BeanProducingAProcessorOfMessages.class);
    SeContainer container = initializer.initialize();
    MyCollector collector = container.select(MyCollector.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

  @Test
  public void testBeanProducingAProcessorBuilder() {
    initializer.addBeanClasses(BeanProducingAProcessorBuilderOfMessages.class);
    SeContainer container = initializer.initialize();
    MyCollector collector = container.select(MyCollector.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }


}
