package io.smallrye.reactive.messaging;

import io.smallrye.reactive.messaging.beans.*;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class PublisherMethodTest extends WeldTestBase {

  @Test
  public void testBeanProducingMessagesAsFlowable() {
    weld.addBeanClass(BeanProducingMessagesAsFlowable.class);
    WeldContainer container = weld.initialize();
    MyCollector collector = container.getBeanManager().createInstance().select(MyCollector.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

  @Test
  public void testBeanProducingPayloadsAsFlowable() {
    weld.addBeanClass(BeanProducingPayloadAsFlowable.class);
    WeldContainer container = weld.initialize();
    MyCollector collector = container.getBeanManager().createInstance().select(MyCollector.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

  @Test
  public void testBeanProducingMessagesAsPublisher() {
    weld.addBeanClass(BeanProducingMessagesAsPublisher.class);
    WeldContainer container = weld.initialize();
    MyCollector collector = container.getBeanManager().createInstance().select(MyCollector.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

  @Test
  public void testBeanProducingPayloadsAsPublisher() {
    weld.addBeanClass(BeanProducingPayloadAsPublisher.class);
    WeldContainer container = weld.initialize();
    MyCollector collector = container.getBeanManager().createInstance().select(MyCollector.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

  @Test
  public void testBeanProducingMessagesAsPublisherBuilder() {
    weld.addBeanClass(BeanProducingMessagesAsPublisherBuilder.class);
    WeldContainer container = weld.initialize();
    MyCollector collector = container.getBeanManager().createInstance().select(MyCollector.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

  @Test
  public void testBeanProducingPayloadAsPublisherBuilder() {
    weld.addBeanClass(BeanProducingPayloadAsPublisherBuilder.class);
    WeldContainer container = weld.initialize();
    MyCollector collector = container.getBeanManager().createInstance().select(MyCollector.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }


}
