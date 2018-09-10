package io.smallrye.reactive.messaging.ack;

import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.Test;

import static io.smallrye.reactive.messaging.ack.BeanWithProcessorsConsumingItems.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class ProcessorConsumingItemsAcknowledgementTest extends WeldTestBaseWithoutTails {

  @Test
  public void testManualAcknowledgement() {
    weld.addBeanClass(BeanWithProcessorsConsumingItems.class);
    WeldContainer container = weld.initialize();
    BeanWithProcessorsConsumingItems bean = container.getBeanManager().createInstance().select(BeanWithProcessorsConsumingItems.class).get();
    await().until(() -> bean.acknowledged(MANUAL_ACKNOWLEDGMENT).size() == 5);
    assertThat(bean.acknowledged(MANUAL_ACKNOWLEDGMENT)).containsExactly("a", "b", "c", "d", "e");
    assertThat(bean.received(MANUAL_ACKNOWLEDGMENT)).containsExactly("a", "b", "c", "d", "e");
  }

  @Test
  public void testNoAcknowledgement() {
    weld.addBeanClass(BeanWithProcessorsConsumingItems.class);
    WeldContainer container = weld.initialize();
    BeanWithProcessorsConsumingItems bean = container.getBeanManager().createInstance().select(BeanWithProcessorsConsumingItems.class).get();
    await().until(() -> bean.received(NO_ACKNOWLEDGMENT).size() == 5);
    assertThat(bean.acknowledged(NO_ACKNOWLEDGMENT)).isNull();
    assertThat(bean.received(NO_ACKNOWLEDGMENT)).containsExactly("a", "b", "c", "d", "e");
  }

  @Test
  public void testAutoAcknowledgement() {
    weld.addBeanClass(BeanWithProcessorsConsumingItems.class);
    WeldContainer container = weld.initialize();
    BeanWithProcessorsConsumingItems bean = container.getBeanManager().createInstance().select(BeanWithProcessorsConsumingItems.class).get();
    await().until(() -> bean.acknowledged(AUTO_ACKNOWLEDGMENT).size() == 5);
    assertThat(bean.acknowledged(AUTO_ACKNOWLEDGMENT)).containsExactly("a", "b", "c", "d", "e");
    assertThat(bean.received(AUTO_ACKNOWLEDGMENT)).containsExactly("a", "b", "c", "d", "e");
  }

  @Test
  public void testManualAcknowledgementWithCS() {
    weld.addBeanClass(BeanWithProcessorsConsumingItems.class);
    WeldContainer container = weld.initialize();
    BeanWithProcessorsConsumingItems bean = container.getBeanManager().createInstance().select(BeanWithProcessorsConsumingItems.class).get();
    await().until(() -> bean.acknowledged(MANUAL_ACKNOWLEDGMENT_CS).size() == 5);
    assertThat(bean.acknowledged(MANUAL_ACKNOWLEDGMENT_CS)).containsExactly("a", "b", "c", "d", "e");
    assertThat(bean.received(MANUAL_ACKNOWLEDGMENT_CS)).containsExactly("a", "b", "c", "d", "e");
  }

  @Test
  public void testNoAcknowledgementWithCS() {
    weld.addBeanClass(BeanWithProcessorsConsumingItems.class);
    WeldContainer container = weld.initialize();
    BeanWithProcessorsConsumingItems bean = container.getBeanManager().createInstance().select(BeanWithProcessorsConsumingItems.class).get();
    await().until(() -> bean.received(NO_ACKNOWLEDGMENT_CS).size() == 5);
    assertThat(bean.acknowledged(NO_ACKNOWLEDGMENT_CS)).isNull();
    assertThat(bean.received(NO_ACKNOWLEDGMENT_CS)).containsExactly("a", "b", "c", "d", "e");
  }

  @Test
  public void testAutoAcknowledgementWithCS() {
    weld.addBeanClass(BeanWithProcessorsConsumingItems.class);
    WeldContainer container = weld.initialize();
    BeanWithProcessorsConsumingItems bean = container.getBeanManager().createInstance().select(BeanWithProcessorsConsumingItems.class).get();
    await().until(() -> bean.acknowledged(AUTO_ACKNOWLEDGMENT_CS).size() == 5);
    assertThat(bean.acknowledged(AUTO_ACKNOWLEDGMENT_CS)).containsExactly("a", "b", "c", "d", "e");
    assertThat(bean.received(AUTO_ACKNOWLEDGMENT_CS)).containsExactly("a", "b", "c", "d", "e");
  }



}
