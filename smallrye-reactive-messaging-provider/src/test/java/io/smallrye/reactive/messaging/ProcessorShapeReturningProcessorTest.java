package io.smallrye.reactive.messaging;

import javax.enterprise.inject.se.SeContainer;

import io.smallrye.reactive.messaging.beans.BeanProducingAProcessorBuilderOfMessages;
import io.smallrye.reactive.messaging.beans.BeanProducingAProcessorBuilderOfPayloads;
import io.smallrye.reactive.messaging.beans.BeanProducingAProcessorOfMessages;
import io.smallrye.reactive.messaging.beans.BeanProducingAProcessorOfPayloads;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ProcessorShapeReturningProcessorTest extends WeldTestBase {

  @Test
  public void testBeanProducingAProcessorOfMessages() {
    initializer.addBeanClasses(BeanProducingAProcessorOfMessages.class);
    SeContainer container = initializer.initialize();
    MyCollector collector = container.select(MyCollector.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

  @Test
  public void testBeanProducingAProcessorBuilderOfMessages() {
    initializer.addBeanClasses(BeanProducingAProcessorBuilderOfMessages.class);
    SeContainer container = initializer.initialize();
    MyCollector collector = container.select(MyCollector.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

  @Test
  public void testBeanProducingAProcessorOfPayloads() {
    initializer.addBeanClasses(BeanProducingAProcessorOfPayloads.class);
    SeContainer container = initializer.initialize();
    MyCollector collector = container.select(MyCollector.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

  @Test
  public void testBeanProducingAProcessorBuilderOfPayloads() {
    initializer.addBeanClasses(BeanProducingAProcessorBuilderOfPayloads.class);
    SeContainer container = initializer.initialize();
    MyCollector collector = container.select(MyCollector.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }


}
