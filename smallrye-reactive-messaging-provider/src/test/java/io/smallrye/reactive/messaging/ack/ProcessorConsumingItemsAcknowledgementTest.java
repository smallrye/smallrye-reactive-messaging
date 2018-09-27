package io.smallrye.reactive.messaging.ack;

import javax.enterprise.inject.se.SeContainer;

import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import org.junit.Test;

import static io.smallrye.reactive.messaging.ack.BeanWithProcessorsConsumingItems.AUTO_ACKNOWLEDGMENT;
import static io.smallrye.reactive.messaging.ack.BeanWithProcessorsConsumingItems.AUTO_ACKNOWLEDGMENT_CS;
import static io.smallrye.reactive.messaging.ack.BeanWithProcessorsConsumingItems.MANUAL_ACKNOWLEDGMENT;
import static io.smallrye.reactive.messaging.ack.BeanWithProcessorsConsumingItems.MANUAL_ACKNOWLEDGMENT_CS;
import static io.smallrye.reactive.messaging.ack.BeanWithProcessorsConsumingItems.NO_ACKNOWLEDGMENT;
import static io.smallrye.reactive.messaging.ack.BeanWithProcessorsConsumingItems.NO_ACKNOWLEDGMENT_CS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class ProcessorConsumingItemsAcknowledgementTest extends WeldTestBaseWithoutTails {

  @Test
  public void testManualAcknowledgement() {
    initializer.addBeanClasses(BeanWithProcessorsConsumingItems.class);
    SeContainer container = initializer.initialize();
    BeanWithProcessorsConsumingItems bean = container.select(BeanWithProcessorsConsumingItems.class).get();
    await().until(() -> bean.acknowledged(MANUAL_ACKNOWLEDGMENT).size() == 5);
    assertThat(bean.acknowledged(MANUAL_ACKNOWLEDGMENT)).containsExactly("a", "b", "c", "d", "e");
    assertThat(bean.received(MANUAL_ACKNOWLEDGMENT)).containsExactly("a", "b", "c", "d", "e");
  }

  @Test
  public void testNoAcknowledgement() {
    initializer.addBeanClasses(BeanWithProcessorsConsumingItems.class);
    SeContainer container = initializer.initialize();
    BeanWithProcessorsConsumingItems bean = container.select(BeanWithProcessorsConsumingItems.class).get();
    await().until(() -> bean.received(NO_ACKNOWLEDGMENT).size() == 5);
    assertThat(bean.acknowledged(NO_ACKNOWLEDGMENT)).isNull();
    assertThat(bean.received(NO_ACKNOWLEDGMENT)).containsExactly("a", "b", "c", "d", "e");
  }

  @Test
  public void testAutoAcknowledgement() {
    initializer.addBeanClasses(BeanWithProcessorsConsumingItems.class);
    SeContainer container = initializer.initialize();
    BeanWithProcessorsConsumingItems bean = container.select(BeanWithProcessorsConsumingItems.class).get();
    await().until(() -> bean.acknowledged(AUTO_ACKNOWLEDGMENT).size() == 5);
    assertThat(bean.acknowledged(AUTO_ACKNOWLEDGMENT)).containsExactly("a", "b", "c", "d", "e");
    assertThat(bean.received(AUTO_ACKNOWLEDGMENT)).containsExactly("a", "b", "c", "d", "e");
  }

  @Test
  public void testManualAcknowledgementWithCS() {
    initializer.addBeanClasses(BeanWithProcessorsConsumingItems.class);
    SeContainer container = initializer.initialize();
    BeanWithProcessorsConsumingItems bean = container.select(BeanWithProcessorsConsumingItems.class).get();
    await().until(() -> bean.acknowledged(MANUAL_ACKNOWLEDGMENT_CS).size() == 5);
    assertThat(bean.acknowledged(MANUAL_ACKNOWLEDGMENT_CS)).containsExactly("a", "b", "c", "d", "e");
    assertThat(bean.received(MANUAL_ACKNOWLEDGMENT_CS)).containsExactly("a", "b", "c", "d", "e");
  }

  @Test
  public void testNoAcknowledgementWithCS() {
    initializer.addBeanClasses(BeanWithProcessorsConsumingItems.class);
    SeContainer container = initializer.initialize();
    BeanWithProcessorsConsumingItems bean = container.select(BeanWithProcessorsConsumingItems.class).get();
    await().until(() -> bean.received(NO_ACKNOWLEDGMENT_CS).size() == 5);
    assertThat(bean.acknowledged(NO_ACKNOWLEDGMENT_CS)).isNull();
    assertThat(bean.received(NO_ACKNOWLEDGMENT_CS)).containsExactly("a", "b", "c", "d", "e");
  }

  @Test
  public void testAutoAcknowledgementWithCS() {
    initializer.addBeanClasses(BeanWithProcessorsConsumingItems.class);
    SeContainer container = initializer.initialize();
    BeanWithProcessorsConsumingItems bean = container.select(BeanWithProcessorsConsumingItems.class).get();
    await().until(() -> bean.acknowledged(AUTO_ACKNOWLEDGMENT_CS).size() == 5);
    assertThat(bean.acknowledged(AUTO_ACKNOWLEDGMENT_CS)).containsExactly("a", "b", "c", "d", "e");
    assertThat(bean.received(AUTO_ACKNOWLEDGMENT_CS)).containsExactly("a", "b", "c", "d", "e");
  }



}
