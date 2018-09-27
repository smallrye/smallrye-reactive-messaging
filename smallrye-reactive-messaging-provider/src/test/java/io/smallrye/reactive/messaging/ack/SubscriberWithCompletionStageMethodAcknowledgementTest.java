package io.smallrye.reactive.messaging.ack;

import javax.enterprise.inject.se.SeContainer;

import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import org.junit.Test;

import static io.smallrye.reactive.messaging.ack.BeanWithSubscriberUsingCompletionStageMethods.AUTO_ACKNOWLEDGMENT;
import static io.smallrye.reactive.messaging.ack.BeanWithSubscriberUsingCompletionStageMethods.MANUAL_ACKNOWLEDGMENT;
import static io.smallrye.reactive.messaging.ack.BeanWithSubscriberUsingCompletionStageMethods.NO_ACKNOWLEDGMENT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class SubscriberWithCompletionStageMethodAcknowledgementTest extends WeldTestBaseWithoutTails {

  @Test
  public void testManualAcknowledgement() {
    initializer.addBeanClasses(BeanWithSubscriberUsingCompletionStageMethods.class);
    SeContainer container = initializer.initialize();
    BeanWithSubscriberUsingCompletionStageMethods bean = container.select(BeanWithSubscriberUsingCompletionStageMethods.class).get();
    await().until(() -> bean.acknowledged(MANUAL_ACKNOWLEDGMENT).size() == 5);
    assertThat(bean.acknowledged(MANUAL_ACKNOWLEDGMENT)).containsExactly("a", "b", "c", "d", "e");
    assertThat(bean.received(MANUAL_ACKNOWLEDGMENT)).containsExactly("a", "b", "c", "d", "e");
  }

  @Test
  public void testNoAcknowledgement() {
    initializer.addBeanClasses(BeanWithSubscriberUsingCompletionStageMethods.class);
    SeContainer container = initializer.initialize();
    BeanWithSubscriberUsingCompletionStageMethods bean = container.select(BeanWithSubscriberUsingCompletionStageMethods.class).get();
    await().until(() -> bean.received(NO_ACKNOWLEDGMENT).size() == 5);
    assertThat(bean.acknowledged(NO_ACKNOWLEDGMENT)).isNull();
    assertThat(bean.received(NO_ACKNOWLEDGMENT)).containsExactly("a", "b", "c", "d", "e");
  }

  @Test
  public void testAutoAcknowledgement() {
    initializer.addBeanClasses(BeanWithSubscriberUsingCompletionStageMethods.class);
    SeContainer container = initializer.initialize();
    BeanWithSubscriberUsingCompletionStageMethods bean = container.select(BeanWithSubscriberUsingCompletionStageMethods.class).get();
    await().until(() -> bean.acknowledged(AUTO_ACKNOWLEDGMENT).size() == 5);
    assertThat(bean.acknowledged(AUTO_ACKNOWLEDGMENT)).containsExactly("a", "b", "c", "d", "e");
    assertThat(bean.received(AUTO_ACKNOWLEDGMENT)).containsExactly("a", "b", "c", "d", "e");
  }



}
