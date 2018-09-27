package io.smallrye.reactive.messaging;

import javax.enterprise.inject.se.SeContainer;

import io.smallrye.reactive.messaging.beans.BeanProducingAPublisherBuilderOfMessagesAndConsumingIndividualMessage;
import io.smallrye.reactive.messaging.beans.BeanProducingAPublisherBuilderOfPayloadsAndConsumingIndividualPayload;
import io.smallrye.reactive.messaging.beans.BeanProducingAPublisherOfMessagesAndConsumingIndividualMessage;
import io.smallrye.reactive.messaging.beans.BeanProducingAPublisherOfPayloadsAndConsumingIndividualPayload;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ProcessorShapeReturningPublisherTest extends WeldTestBase {

  @Test
  public void testBeanProducingAPublisherOfMessagesAndConsumingIndividualMessage() {
    initializer.addBeanClasses(BeanProducingAPublisherOfMessagesAndConsumingIndividualMessage.class);
    SeContainer container = initializer.initialize();
    MyCollector collector = container.select(MyCollector.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

  @Test
  public void testBeanProducingAPublisherOfPayloadsAndConsumingIndividualPayload() {
    initializer.addBeanClasses(BeanProducingAPublisherOfPayloadsAndConsumingIndividualPayload.class);
    SeContainer container = initializer.initialize();
    MyCollector collector = container.select(MyCollector.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

  @Test
  public void testBeanProducingAPublisherBuilderOfPayloadsAndConsumingIndividualPayload() {
    initializer.addBeanClasses(BeanProducingAPublisherBuilderOfPayloadsAndConsumingIndividualPayload.class);
    SeContainer container = initializer.initialize();
    MyCollector collector = container.select(MyCollector.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

  @Test
  public void testBeanProducingAPublisherBuilderOfMessagesAndConsumingIndividualMessage() {
    initializer.addBeanClasses(BeanProducingAPublisherBuilderOfMessagesAndConsumingIndividualMessage.class);
    SeContainer container = initializer.initialize();
    MyCollector collector = container.select(MyCollector.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

}
