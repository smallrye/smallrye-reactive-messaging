package io.smallrye.reactive.messaging.ack;

import javax.enterprise.inject.se.SeContainer;

import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import org.junit.Test;

import static io.smallrye.reactive.messaging.ack.BeanWithProcessorsProducingStreams.AUTO_ACKNOWLEDGMENT;
import static io.smallrye.reactive.messaging.ack.BeanWithProcessorsProducingStreams.AUTO_ACKNOWLEDGMENT_BUILDER;
import static io.smallrye.reactive.messaging.ack.BeanWithProcessorsProducingStreams.MANUAL_ACKNOWLEDGMENT;
import static io.smallrye.reactive.messaging.ack.BeanWithProcessorsProducingStreams.MANUAL_ACKNOWLEDGMENT_BUILDER;
import static io.smallrye.reactive.messaging.ack.BeanWithProcessorsProducingStreams.NO_ACKNOWLEDGMENT;
import static io.smallrye.reactive.messaging.ack.BeanWithProcessorsProducingStreams.NO_ACKNOWLEDGMENT_BUILDER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class ProcessorProducingStreamsAcknowledgementTest extends WeldTestBaseWithoutTails {

  @Test
  public void testManualAcknowledgement() {
    initializer.addBeanClasses(BeanWithProcessorsProducingStreams.class);
    SeContainer container = initializer.initialize();
    BeanWithProcessorsProducingStreams bean = container.select(BeanWithProcessorsProducingStreams.class).get();
    await().until(() -> bean.acknowledged(MANUAL_ACKNOWLEDGMENT).size() == 5);
    assertThat(bean.acknowledged(MANUAL_ACKNOWLEDGMENT)).containsExactly("a", "b", "c", "d", "e");
    assertThat(bean.received(MANUAL_ACKNOWLEDGMENT)).containsExactly("a", "b", "c", "d", "e");
  }

  @Test
  public void testNoAcknowledgement() {
    initializer.addBeanClasses(BeanWithProcessorsProducingStreams.class);
    SeContainer container = initializer.initialize();
    BeanWithProcessorsProducingStreams bean = container.select(BeanWithProcessorsProducingStreams.class).get();
    await().until(() -> bean.received(NO_ACKNOWLEDGMENT).size() == 5);
    assertThat(bean.acknowledged(NO_ACKNOWLEDGMENT)).isNull();
    assertThat(bean.received(NO_ACKNOWLEDGMENT)).containsExactly("a", "b", "c", "d", "e");
  }

  @Test
  public void testAutoAcknowledgement() {
    initializer.addBeanClasses(BeanWithProcessorsProducingStreams.class);
    SeContainer container = initializer.initialize();
    BeanWithProcessorsProducingStreams bean = container.select(BeanWithProcessorsProducingStreams.class).get();
    await().until(() -> bean.acknowledged(AUTO_ACKNOWLEDGMENT).size() == 5);
    assertThat(bean.acknowledged(AUTO_ACKNOWLEDGMENT)).containsExactly("a", "b", "c", "d", "e");
    assertThat(bean.received(AUTO_ACKNOWLEDGMENT)).containsExactly("a", "b", "c", "d", "e");
  }

  @Test
  public void testManualAcknowledgementWithBuilder() {
    initializer.addBeanClasses(BeanWithProcessorsProducingStreams.class);
    SeContainer container = initializer.initialize();
    BeanWithProcessorsProducingStreams bean = container.select(BeanWithProcessorsProducingStreams.class).get();
    await().until(() -> bean.acknowledged(MANUAL_ACKNOWLEDGMENT_BUILDER).size() == 5);
    assertThat(bean.acknowledged(MANUAL_ACKNOWLEDGMENT_BUILDER)).containsExactly("a", "b", "c", "d", "e");
    assertThat(bean.received(MANUAL_ACKNOWLEDGMENT_BUILDER)).containsExactly("a", "b", "c", "d", "e");
  }

  @Test
  public void testNoAcknowledgementWithBuilder() {
    initializer.addBeanClasses(BeanWithProcessorsProducingStreams.class);
    SeContainer container = initializer.initialize();
    BeanWithProcessorsProducingStreams bean = container.select(BeanWithProcessorsProducingStreams.class).get();
    await().until(() -> bean.received(NO_ACKNOWLEDGMENT_BUILDER).size() == 5);
    assertThat(bean.acknowledged(NO_ACKNOWLEDGMENT_BUILDER)).isNull();
    assertThat(bean.received(NO_ACKNOWLEDGMENT_BUILDER)).containsExactly("a", "b", "c", "d", "e");
  }

  @Test
  public void testAutoAcknowledgementWithBuilder() {
    initializer.addBeanClasses(BeanWithProcessorsProducingStreams.class);
    SeContainer container = initializer.initialize();
    BeanWithProcessorsProducingStreams bean = container.select(BeanWithProcessorsProducingStreams.class).get();
    await().until(() -> bean.acknowledged(AUTO_ACKNOWLEDGMENT_BUILDER).size() == 5);
    assertThat(bean.acknowledged(AUTO_ACKNOWLEDGMENT_BUILDER)).containsExactly("a", "b", "c", "d", "e");
    assertThat(bean.received(AUTO_ACKNOWLEDGMENT_BUILDER)).containsExactly("a", "b", "c", "d", "e");
  }



}
