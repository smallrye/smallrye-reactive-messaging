package io.smallrye.reactive.messaging;

import io.smallrye.reactive.messaging.beans.BeanConsumingMsgAsFlowableAndPublishingMsgAsFlowable;
import io.smallrye.reactive.messaging.beans.BeanConsumingMsgAsFlowableAndPublishingMsgAsPublisher;
import io.smallrye.reactive.messaging.beans.BeanConsumingMsgAsPublisherAndPublishingMsgAsFlowable;
import io.smallrye.reactive.messaging.beans.BeanConsumingMsgAsPublisherBuilderAndPublishingMsgAsPublisherBuilder;
import io.smallrye.reactive.messaging.beans.BeanProducingAProcessorBuilderOfMessages;
import io.smallrye.reactive.messaging.beans.BeanProducingAProcessorOfMessages;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class StreamTransformerShapeTest extends WeldTestBase {

  @Test
  public void testBeanConsumingMsgAsFlowableAndPublishingMsgAsFlowable() {
    addBeanClass(BeanConsumingMsgAsFlowableAndPublishingMsgAsFlowable.class);
    initialize();
    MyCollector collector = container.select(MyCollector.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

  @Test
  public void testBeanConsumingMsgAsFlowableAndPublishingMsgAsPublisher() {
    addBeanClass(BeanConsumingMsgAsFlowableAndPublishingMsgAsPublisher.class);
    initialize();
    MyCollector collector = container.select(MyCollector.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

  @Test
  public void testBeanConsumingMsgAsPublisherAndPublishingMsgAsFlowable() {
    addBeanClass(BeanConsumingMsgAsPublisherAndPublishingMsgAsFlowable.class);
    initialize();
    MyCollector collector = container.select(MyCollector.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

  @Test
  public void testBeanConsumingMsgAsPublisherBuilderAndPublishingMsgAsPublisherBuilder() {
    addBeanClass(BeanConsumingMsgAsPublisherBuilderAndPublishingMsgAsPublisherBuilder.class);
    initialize();
    MyCollector collector = container.select(MyCollector.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

  @Test
  public void testBeanProducingAProcessor() {
    addBeanClass(BeanProducingAProcessorOfMessages.class);
    initialize();
    MyCollector collector = container.select(MyCollector.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

  @Test
  public void testBeanProducingAProcessorBuilder() {
    addBeanClass(BeanProducingAProcessorBuilderOfMessages.class);
    initialize();
    MyCollector collector = container.select(MyCollector.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }


}
