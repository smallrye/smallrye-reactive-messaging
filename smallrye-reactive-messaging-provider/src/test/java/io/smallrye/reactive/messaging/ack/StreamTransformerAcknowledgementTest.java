package io.smallrye.reactive.messaging.ack;

import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.Test;

import static io.smallrye.reactive.messaging.ack.BeanWithStreamTransformers.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class StreamTransformerAcknowledgementTest extends WeldTestBaseWithoutTails {

  @Test
  public void testManualAcknowledgement() {
    weld.addBeanClass(BeanWithStreamTransformers.class);
    WeldContainer container = weld.initialize();
    BeanWithStreamTransformers bean = container.getBeanManager().createInstance().select(BeanWithStreamTransformers.class).get();
    await().until(() -> bean.acknowledged(MANUAL_ACKNOWLEDGMENT).size() == 5);
    assertThat(bean.acknowledged(MANUAL_ACKNOWLEDGMENT)).containsExactly("a", "b", "c", "d", "e");
    assertThat(bean.received(MANUAL_ACKNOWLEDGMENT)).containsExactly("a", "b", "c", "d", "e");
  }

  @Test
  public void testNoAcknowledgement() {
    weld.addBeanClass(BeanWithStreamTransformers.class);
    WeldContainer container = weld.initialize();
    BeanWithStreamTransformers bean = container.getBeanManager().createInstance().select(BeanWithStreamTransformers.class).get();
    await().until(() -> bean.received(NO_ACKNOWLEDGMENT).size() == 5);
    assertThat(bean.acknowledged(NO_ACKNOWLEDGMENT)).isNull();
    assertThat(bean.received(NO_ACKNOWLEDGMENT)).containsExactly("a", "b", "c", "d", "e");
  }

  @Test
  public void testManualAcknowledgementWithBuilder() {
    weld.addBeanClass(BeanWithStreamTransformers.class);
    WeldContainer container = weld.initialize();
    BeanWithStreamTransformers bean = container.getBeanManager().createInstance().select(BeanWithStreamTransformers.class).get();
    await().until(() -> bean.acknowledged(MANUAL_ACKNOWLEDGMENT_BUILDER).size() == 5);
    assertThat(bean.acknowledged(MANUAL_ACKNOWLEDGMENT_BUILDER)).containsExactly("a", "b", "c", "d", "e");
    assertThat(bean.received(MANUAL_ACKNOWLEDGMENT_BUILDER)).containsExactly("a", "b", "c", "d", "e");
  }

  @Test
  public void testNoAcknowledgementWithBuilder() {
    weld.addBeanClass(BeanWithStreamTransformers.class);
    WeldContainer container = weld.initialize();
    BeanWithStreamTransformers bean = container.getBeanManager().createInstance().select(BeanWithStreamTransformers.class).get();
    await().until(() -> bean.received(NO_ACKNOWLEDGMENT_BUILDER).size() == 5);
    assertThat(bean.acknowledged(NO_ACKNOWLEDGMENT_BUILDER)).isNull();
    assertThat(bean.received(NO_ACKNOWLEDGMENT_BUILDER)).containsExactly("a", "b", "c", "d", "e");
  }


}
