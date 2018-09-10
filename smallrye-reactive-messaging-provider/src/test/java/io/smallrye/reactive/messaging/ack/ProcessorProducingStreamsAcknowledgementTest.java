package io.smallrye.reactive.messaging.ack;

import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.Test;

import static io.smallrye.reactive.messaging.ack.BeanWithProcessorsProducingStreams.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class ProcessorProducingStreamsAcknowledgementTest extends WeldTestBaseWithoutTails {

  @Test
  public void testManualAcknowledgement() {
    weld.addBeanClass(BeanWithProcessorsProducingStreams.class);
    WeldContainer container = weld.initialize();
    BeanWithProcessorsProducingStreams bean = container.getBeanManager().createInstance().select(BeanWithProcessorsProducingStreams.class).get();
    await().until(() -> bean.acknowledged(MANUAL_ACKNOWLEDGMENT).size() == 5);
    assertThat(bean.acknowledged(MANUAL_ACKNOWLEDGMENT)).containsExactly("a", "b", "c", "d", "e");
    assertThat(bean.received(MANUAL_ACKNOWLEDGMENT)).containsExactly("a", "b", "c", "d", "e");
  }

  @Test
  public void testNoAcknowledgement() {
    weld.addBeanClass(BeanWithProcessorsProducingStreams.class);
    WeldContainer container = weld.initialize();
    BeanWithProcessorsProducingStreams bean = container.getBeanManager().createInstance().select(BeanWithProcessorsProducingStreams.class).get();
    await().until(() -> bean.received(NO_ACKNOWLEDGMENT).size() == 5);
    assertThat(bean.acknowledged(NO_ACKNOWLEDGMENT)).isNull();
    assertThat(bean.received(NO_ACKNOWLEDGMENT)).containsExactly("a", "b", "c", "d", "e");
  }

  @Test
  public void testAutoAcknowledgement() {
    weld.addBeanClass(BeanWithProcessorsProducingStreams.class);
    WeldContainer container = weld.initialize();
    BeanWithProcessorsProducingStreams bean = container.getBeanManager().createInstance().select(BeanWithProcessorsProducingStreams.class).get();
    await().until(() -> bean.acknowledged(AUTO_ACKNOWLEDGMENT).size() == 5);
    assertThat(bean.acknowledged(AUTO_ACKNOWLEDGMENT)).containsExactly("a", "b", "c", "d", "e");
    assertThat(bean.received(AUTO_ACKNOWLEDGMENT)).containsExactly("a", "b", "c", "d", "e");
  }

  @Test
  public void testManualAcknowledgementWithBuilder() {
    weld.addBeanClass(BeanWithProcessorsProducingStreams.class);
    WeldContainer container = weld.initialize();
    BeanWithProcessorsProducingStreams bean = container.getBeanManager().createInstance().select(BeanWithProcessorsProducingStreams.class).get();
    await().until(() -> bean.acknowledged(MANUAL_ACKNOWLEDGMENT_BUILDER).size() == 5);
    assertThat(bean.acknowledged(MANUAL_ACKNOWLEDGMENT_BUILDER)).containsExactly("a", "b", "c", "d", "e");
    assertThat(bean.received(MANUAL_ACKNOWLEDGMENT_BUILDER)).containsExactly("a", "b", "c", "d", "e");
  }

  @Test
  public void testNoAcknowledgementWithBuilder() {
    weld.addBeanClass(BeanWithProcessorsProducingStreams.class);
    WeldContainer container = weld.initialize();
    BeanWithProcessorsProducingStreams bean = container.getBeanManager().createInstance().select(BeanWithProcessorsProducingStreams.class).get();
    await().until(() -> bean.received(NO_ACKNOWLEDGMENT_BUILDER).size() == 5);
    assertThat(bean.acknowledged(NO_ACKNOWLEDGMENT_BUILDER)).isNull();
    assertThat(bean.received(NO_ACKNOWLEDGMENT_BUILDER)).containsExactly("a", "b", "c", "d", "e");
  }

  @Test
  public void testAutoAcknowledgementWithBuilder() {
    weld.addBeanClass(BeanWithProcessorsProducingStreams.class);
    WeldContainer container = weld.initialize();
    BeanWithProcessorsProducingStreams bean = container.getBeanManager().createInstance().select(BeanWithProcessorsProducingStreams.class).get();
    await().until(() -> bean.acknowledged(AUTO_ACKNOWLEDGMENT_BUILDER).size() == 5);
    assertThat(bean.acknowledged(AUTO_ACKNOWLEDGMENT_BUILDER)).containsExactly("a", "b", "c", "d", "e");
    assertThat(bean.received(AUTO_ACKNOWLEDGMENT_BUILDER)).containsExactly("a", "b", "c", "d", "e");
  }



}
