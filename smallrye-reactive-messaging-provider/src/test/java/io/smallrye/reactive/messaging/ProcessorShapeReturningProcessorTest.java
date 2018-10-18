package io.smallrye.reactive.messaging;

import io.smallrye.reactive.messaging.beans.BeanProducingAProcessorBuilderOfMessages;
import io.smallrye.reactive.messaging.beans.BeanProducingAProcessorBuilderOfPayloads;
import io.smallrye.reactive.messaging.beans.BeanProducingAProcessorOfMessages;
import io.smallrye.reactive.messaging.beans.BeanProducingAProcessorOfPayloads;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ProcessorShapeReturningProcessorTest extends WeldTestBase {

  @Test
  public void testBeanProducingAProcessorOfMessages() {
    addBeanClass(BeanProducingAProcessorOfMessages.class);
    initialize();
    MyCollector collector = container.select(MyCollector.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

  @Test
  public void testBeanProducingAProcessorBuilderOfMessages() {
    addBeanClass(BeanProducingAProcessorBuilderOfMessages.class);
    initialize();
    MyCollector collector = container.select(MyCollector.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

  @Test
  public void testBeanProducingAProcessorOfPayloads() {
    addBeanClass(BeanProducingAProcessorOfPayloads.class);
    initialize();
    MyCollector collector = container.select(MyCollector.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }

  @Test
  public void testBeanProducingAProcessorBuilderOfPayloads() {
    addBeanClass(BeanProducingAProcessorBuilderOfPayloads.class);
    initialize();
    MyCollector collector = container.select(MyCollector.class).get();
    assertThat(collector.payloads()).isEqualTo(EXPECTED);
  }


}
