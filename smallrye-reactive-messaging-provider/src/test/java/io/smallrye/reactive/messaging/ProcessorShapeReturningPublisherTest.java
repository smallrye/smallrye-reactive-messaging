package io.smallrye.reactive.messaging;

import io.smallrye.reactive.messaging.beans.BeanProducingAPublisherBuilderOfMessagesAndConsumingIndividualMessage;
import io.smallrye.reactive.messaging.beans.BeanProducingAPublisherBuilderOfPayloadsAndConsumingIndividualPayload;
import io.smallrye.reactive.messaging.beans.BeanProducingAPublisherOfMessagesAndConsumingIndividualMessage;
import io.smallrye.reactive.messaging.beans.BeanProducingAPublisherOfPayloadsAndConsumingIndividualPayload;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ProcessorShapeReturningPublisherTest extends WeldTestBase {

  @Test
  public void testBeanProducingAPublisherOfMessagesAndConsumingIndividualMessage() {
    weld.addBeanClass(BeanProducingAPublisherOfMessagesAndConsumingIndividualMessage.class);
    WeldContainer container = weld.initialize();
    MyCollector collector = container.getBeanManager().createInstance().select(MyCollector.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

  @Test
  public void testBeanProducingAPublisherOfPayloadsAndConsumingIndividualPayload() {
    weld.addBeanClass(BeanProducingAPublisherOfPayloadsAndConsumingIndividualPayload.class);
    WeldContainer container = weld.initialize();
    MyCollector collector = container.getBeanManager().createInstance().select(MyCollector.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

  @Test
  public void testBeanProducingAPublisherBuilderOfPayloadsAndConsumingIndividualPayload() {
    weld.addBeanClass(BeanProducingAPublisherBuilderOfPayloadsAndConsumingIndividualPayload.class);
    WeldContainer container = weld.initialize();
    MyCollector collector = container.getBeanManager().createInstance().select(MyCollector.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

  @Test
  public void testBeanProducingAPublisherBuilderOfMessagesAndConsumingIndividualMessage() {
    weld.addBeanClass(BeanProducingAPublisherBuilderOfMessagesAndConsumingIndividualMessage.class);
    WeldContainer container = weld.initialize();
    MyCollector collector = container.getBeanManager().createInstance().select(MyCollector.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

}
