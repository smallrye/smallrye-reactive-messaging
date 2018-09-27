package io.smallrye.reactive.messaging.ack;

import javax.enterprise.inject.se.SeContainer;

import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import org.junit.Test;

import static io.smallrye.reactive.messaging.ack.BeanWithSubscriberUsingVoidMethods.AUTO_ACKNOWLEDGMENT;
import static io.smallrye.reactive.messaging.ack.BeanWithSubscriberUsingVoidMethods.MANUAL_ACKNOWLEDGMENT;
import static io.smallrye.reactive.messaging.ack.BeanWithSubscriberUsingVoidMethods.NO_ACKNOWLEDGMENT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class SubscriberWithVoidMethodAcknowledgementTest extends WeldTestBaseWithoutTails {

  @Test
  public void testManualAcknowledgement() {
    initializer.addBeanClasses(BeanWithSubscriberUsingVoidMethods.class);
    SeContainer container = initializer.initialize();
    BeanWithSubscriberUsingVoidMethods bean = container.select(BeanWithSubscriberUsingVoidMethods.class).get();
    await().until(() -> bean.acknowledged(MANUAL_ACKNOWLEDGMENT).size() == 5);
    assertThat(bean.acknowledged(MANUAL_ACKNOWLEDGMENT)).containsExactly("a", "b", "c", "d", "e");
    assertThat(bean.received(MANUAL_ACKNOWLEDGMENT)).containsExactly("a", "b", "c", "d", "e");
  }

  @Test
  public void testNoAcknowledgement() {
    initializer.addBeanClasses(BeanWithSubscriberUsingVoidMethods.class);
    SeContainer container = initializer.initialize();
    BeanWithSubscriberUsingVoidMethods bean = container.select(BeanWithSubscriberUsingVoidMethods.class).get();
    await().until(() -> bean.received(NO_ACKNOWLEDGMENT).size() == 5);
    assertThat(bean.acknowledged(NO_ACKNOWLEDGMENT)).isNull();
    assertThat(bean.received(NO_ACKNOWLEDGMENT)).containsExactly("a", "b", "c", "d", "e");
  }

  @Test
  public void testAutoAcknowledgement() {
    initializer.addBeanClasses(BeanWithSubscriberUsingVoidMethods.class);
    SeContainer container = initializer.initialize();
    BeanWithSubscriberUsingVoidMethods bean = container.select(BeanWithSubscriberUsingVoidMethods.class).get();
    await().until(() -> bean.acknowledged(AUTO_ACKNOWLEDGMENT).size() == 5);
    assertThat(bean.acknowledged(AUTO_ACKNOWLEDGMENT)).containsExactly("a", "b", "c", "d", "e");
    assertThat(bean.received(AUTO_ACKNOWLEDGMENT)).containsExactly("a", "b", "c", "d", "e");
  }



}
