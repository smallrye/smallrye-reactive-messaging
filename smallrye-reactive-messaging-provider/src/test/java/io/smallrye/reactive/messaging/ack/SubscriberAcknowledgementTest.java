package io.smallrye.reactive.messaging.ack;

import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.Test;

import static io.smallrye.reactive.messaging.ack.BeanWithSubscribers.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class SubscriberAcknowledgementTest extends WeldTestBaseWithoutTails {

  @Test
  public void testManualAcknowledgementWithMethodReturningSubscriber() {
    weld.addBeanClass(BeanWithSubscribers.class);
    WeldContainer container = weld.initialize();
    BeanWithSubscribers bean = container.getBeanManager().createInstance().select(BeanWithSubscribers.class).get();
    await().until(() -> bean.acknowledged(MANUAL_ACKNOWLEDGMENT).size() == 5);
    assertThat(bean.acknowledged(MANUAL_ACKNOWLEDGMENT)).containsExactly("a", "b", "c", "d", "e");
    assertThat(bean.received(MANUAL_ACKNOWLEDGMENT)).containsExactly("a", "b", "c", "d", "e");
  }

  @Test
  public void testNoAcknowledgementWithMethodReturningSubscriber() {
    weld.addBeanClass(BeanWithSubscribers.class);
    WeldContainer container = weld.initialize();
    BeanWithSubscribers bean = container.getBeanManager().createInstance().select(BeanWithSubscribers.class).get();
    await().until(() -> bean.received(NO_ACKNOWLEDGMENT).size() == 5);
    assertThat(bean.acknowledged(NO_ACKNOWLEDGMENT)).isNull();
    assertThat(bean.received(NO_ACKNOWLEDGMENT)).containsExactly("a", "b", "c", "d", "e");
  }

  @Test
  public void testPreProcessingAcknowledgementWithMethodReturningSubscriber() {
    weld.addBeanClass(BeanWithSubscribers.class);
    WeldContainer container = weld.initialize();
    BeanWithSubscribers bean = container.getBeanManager().createInstance().select(BeanWithSubscribers.class).get();
    await().until(() -> bean.acknowledged(PRE_PROCESSING_ACK).size() == 5);
    assertThat(bean.acknowledged(PRE_PROCESSING_ACK)).containsExactly("a", "b", "c", "d", "e");
    assertThat(bean.received(PRE_PROCESSING_ACK)).containsExactly("a", "b", "c", "d", "e");
    assertThat(bean.acknowledgeTimeStamps(PRE_PROCESSING_ACK).get(0)).isLessThan(bean.receivedTimeStamps(PRE_PROCESSING_ACK).get(0));
  }

  @Test
  public void testPostProcessingAcknowledgementWithMethodReturningSubscriber() {
    weld.addBeanClass(BeanWithSubscribers.class);
    WeldContainer container = weld.initialize();
    BeanWithSubscribers bean = container.getBeanManager().createInstance().select(BeanWithSubscribers.class).get();
    await().until(() -> bean.acknowledged(POST_PROCESSING_ACK).size() == 5);
    assertThat(bean.acknowledged(POST_PROCESSING_ACK)).containsExactly("a", "b", "c", "d", "e");
    assertThat(bean.received(POST_PROCESSING_ACK)).containsExactly("a", "b", "c", "d", "e");
    assertThat(bean.acknowledgeTimeStamps(POST_PROCESSING_ACK).get(0)).isGreaterThan(bean.receivedTimeStamps(POST_PROCESSING_ACK).get(0));
  }

  @Test
  public void testDefaultProcessingAcknowledgementWithMethodReturningSubscriber() {
    weld.addBeanClass(BeanWithSubscribers.class);
    WeldContainer container = weld.initialize();
    BeanWithSubscribers bean = container.getBeanManager().createInstance().select(BeanWithSubscribers.class).get();
    await().until(() -> bean.acknowledged(DEFAULT_PROCESSING_ACK).size() == 5);
    assertThat(bean.acknowledged(DEFAULT_PROCESSING_ACK)).containsExactly("a", "b", "c", "d", "e");
    assertThat(bean.received(DEFAULT_PROCESSING_ACK)).containsExactly("a", "b", "c", "d", "e");
    assertThat(bean.acknowledgeTimeStamps(POST_PROCESSING_ACK).get(0)).isGreaterThan(bean.receivedTimeStamps(POST_PROCESSING_ACK).get(0));
  }



}
