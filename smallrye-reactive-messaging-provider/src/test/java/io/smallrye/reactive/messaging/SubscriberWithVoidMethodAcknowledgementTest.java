package io.smallrye.reactive.messaging;

import io.smallrye.reactive.messaging.ack.BeanWithSubscriberUsingVoidMethods;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.Test;

import static io.smallrye.reactive.messaging.ack.BeanWithSubscriberUsingVoidMethods.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class SubscriberWithVoidMethodAcknowledgementTest extends WeldTestBaseWithoutTails {

  @Test
  public void testManualAcknowledgement() {
    weld.addBeanClass(BeanWithSubscriberUsingVoidMethods.class);
    WeldContainer container = weld.initialize();
    BeanWithSubscriberUsingVoidMethods bean = container.getBeanManager().createInstance().select(BeanWithSubscriberUsingVoidMethods.class).get();
    await().until(() -> bean.acknowledged(MANUAL_ACKNOWLEDGMENT).size() == 5);
    assertThat(bean.acknowledged(MANUAL_ACKNOWLEDGMENT)).containsExactly("a", "b", "c", "d", "e");
    assertThat(bean.received(MANUAL_ACKNOWLEDGMENT)).containsExactly("a", "b", "c", "d", "e");
  }

  @Test
  public void testNoAcknowledgement() {
    weld.addBeanClass(BeanWithSubscriberUsingVoidMethods.class);
    WeldContainer container = weld.initialize();
    BeanWithSubscriberUsingVoidMethods bean = container.getBeanManager().createInstance().select(BeanWithSubscriberUsingVoidMethods.class).get();
    await().until(() -> bean.received(NO_ACKNOWLEDGMENT).size() == 5);
    assertThat(bean.acknowledged(NO_ACKNOWLEDGMENT)).isNull();
    assertThat(bean.received(NO_ACKNOWLEDGMENT)).containsExactly("a", "b", "c", "d", "e");
  }

  @Test
  public void testAutoAcknowledgement() {
    weld.addBeanClass(BeanWithSubscriberUsingVoidMethods.class);
    WeldContainer container = weld.initialize();
    BeanWithSubscriberUsingVoidMethods bean = container.getBeanManager().createInstance().select(BeanWithSubscriberUsingVoidMethods.class).get();
    await().until(() -> bean.acknowledged(AUTO_ACKNOWLEDGMENT).size() == 5);
    assertThat(bean.acknowledged(AUTO_ACKNOWLEDGMENT)).containsExactly("a", "b", "c", "d", "e");
    assertThat(bean.received(AUTO_ACKNOWLEDGMENT)).containsExactly("a", "b", "c", "d", "e");
  }



}
