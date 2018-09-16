package io.smallrye.reactive.messaging.ack;

import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.Test;

import static io.smallrye.reactive.messaging.ack.BeanWithMethodsReturningSubscribers.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class SubscriberAcknowledgementTest extends WeldTestBaseWithoutTails {

  private <T> T installInitializeAndGet(Class<T> beanClass) {
    weld.addBeanClass(beanClass);
    WeldContainer container = weld.initialize();
    return container.getBeanManager().createInstance().select(beanClass).get();
  }

  @Test
  public void testManualAcknowledgementWithMethodReturningSubscriber() {
    BeanWithMethodsReturningSubscribers bean = installInitializeAndGet(BeanWithMethodsReturningSubscribers.class);
    await().until(() -> bean.acknowledged(MANUAL_ACKNOWLEDGMENT_MESSAGE).size() == 5);
    await().until(() -> bean.received(MANUAL_ACKNOWLEDGMENT_MESSAGE).size() == 5);
    assertThat(bean.acknowledged(MANUAL_ACKNOWLEDGMENT_MESSAGE)).containsExactly("a", "b", "c", "d", "e");
    assertThat(bean.received(MANUAL_ACKNOWLEDGMENT_MESSAGE)).containsExactly("a", "b", "c", "d", "e");
  }

  @Test
  public void testNoAcknowledgementMessageWithMethodReturningSubscriber() {
    BeanWithMethodsReturningSubscribers bean = installInitializeAndGet(BeanWithMethodsReturningSubscribers.class);
    await().until(() -> bean.received(NO_ACKNOWLEDGMENT_MESSAGE).size() == 5);
    assertThat(bean.acknowledged(NO_ACKNOWLEDGMENT_MESSAGE)).isEmpty();
    assertThat(bean.received(NO_ACKNOWLEDGMENT_MESSAGE)).containsExactly("a", "b", "c", "d", "e");
  }

  @Test
  public void testNoAcknowledgementPayloadWithMethodReturningSubscriber() {
    BeanWithMethodsReturningSubscribers bean = installInitializeAndGet(BeanWithMethodsReturningSubscribers.class);
    await().until(() -> bean.received(NO_ACKNOWLEDGMENT_PAYLOAD).size() == 5);
    assertThat(bean.acknowledged(NO_ACKNOWLEDGMENT_PAYLOAD)).isEmpty();
    assertThat(bean.received(NO_ACKNOWLEDGMENT_PAYLOAD)).containsExactly("a", "b", "c", "d", "e");
  }

  @Test
  public void testPreProcessingAcknowledgementMessageWithMethodReturningSubscriber() {
    BeanWithMethodsReturningSubscribers bean = installInitializeAndGet(BeanWithMethodsReturningSubscribers.class);
    await().until(() -> bean.acknowledged(PRE_PROCESSING_ACK_MESSAGE).size() == 5);
    assertThat(bean.acknowledged(PRE_PROCESSING_ACK_MESSAGE)).containsExactly("a", "b", "c", "d", "e");
    assertThat(bean.received(PRE_PROCESSING_ACK_MESSAGE)).containsExactly("a", "b", "c", "d", "e");
    assertThat(bean.acknowledgeTimeStamps(PRE_PROCESSING_ACK_MESSAGE).get(0)).isLessThan(bean.receivedTimeStamps(PRE_PROCESSING_ACK_MESSAGE).get(0));
  }

  @Test
  public void testPreProcessingAcknowledgementPayloadWithMethodReturningSubscriber() {
    BeanWithMethodsReturningSubscribers bean = installInitializeAndGet(BeanWithMethodsReturningSubscribers.class);
    await().until(() -> bean.acknowledged(PRE_PROCESSING_ACK_PAYLOAD).size() == 5);
    assertThat(bean.acknowledged(PRE_PROCESSING_ACK_PAYLOAD)).containsExactly("a", "b", "c", "d", "e");
    assertThat(bean.received(PRE_PROCESSING_ACK_PAYLOAD)).containsExactly("a", "b", "c", "d", "e");
    assertThat(bean.acknowledgeTimeStamps(PRE_PROCESSING_ACK_PAYLOAD).get(0)).isLessThan(bean.receivedTimeStamps(PRE_PROCESSING_ACK_PAYLOAD).get(0));
  }

  @Test
  public void testPostProcessingAcknowledgementMessageWithMethodReturningSubscriber() {
    BeanWithMethodsReturningSubscribers bean = installInitializeAndGet(BeanWithMethodsReturningSubscribers.class);
    await().until(() -> bean.acknowledged(POST_PROCESSING_ACK_MESSAGE).size() == 5);
    assertThat(bean.acknowledged(POST_PROCESSING_ACK_MESSAGE)).containsExactly("a", "b", "c", "d", "e");
    assertThat(bean.received(POST_PROCESSING_ACK_MESSAGE)).containsExactly("a", "b", "c", "d", "e");
    assertThat(bean.acknowledgeTimeStamps(POST_PROCESSING_ACK_MESSAGE).get(0)).isGreaterThan(bean.receivedTimeStamps(POST_PROCESSING_ACK_MESSAGE).get(0));
  }

  @Test
  public void testPostProcessingAcknowledgementPayloadWithMethodReturningSubscriber() {
    BeanWithMethodsReturningSubscribers bean = installInitializeAndGet(BeanWithMethodsReturningSubscribers.class);
    await().until(() -> bean.acknowledged(POST_PROCESSING_ACK_PAYLOAD).size() == 5);
    assertThat(bean.acknowledged(POST_PROCESSING_ACK_PAYLOAD)).containsExactly("a", "b", "c", "d", "e");
    assertThat(bean.received(POST_PROCESSING_ACK_PAYLOAD)).containsExactly("a", "b", "c", "d", "e");
    assertThat(bean.acknowledgeTimeStamps(POST_PROCESSING_ACK_PAYLOAD).get(0)).isGreaterThan(bean.receivedTimeStamps(POST_PROCESSING_ACK_PAYLOAD).get(0));
  }

  @Test
  public void testDefaultProcessingAcknowledgementMessageWithMethodReturningSubscriber() {
    BeanWithMethodsReturningSubscribers bean = installInitializeAndGet(BeanWithMethodsReturningSubscribers.class);
    await().until(() -> bean.acknowledged(DEFAULT_PROCESSING_ACK_MESSAGE).size() == 5);
    assertThat(bean.acknowledged(DEFAULT_PROCESSING_ACK_MESSAGE)).containsExactly("a", "b", "c", "d", "e");
    assertThat(bean.received(DEFAULT_PROCESSING_ACK_MESSAGE)).containsExactly("a", "b", "c", "d", "e");
    assertThat(bean.acknowledgeTimeStamps(DEFAULT_PROCESSING_ACK_MESSAGE).get(0)).isGreaterThan(bean.receivedTimeStamps(DEFAULT_PROCESSING_ACK_MESSAGE).get(0));
  }

  @Test
  public void testDefaultProcessingAcknowledgementPayloadMessageWithMethodReturningSubscriber() {
    BeanWithMethodsReturningSubscribers bean = installInitializeAndGet(BeanWithMethodsReturningSubscribers.class);
    await().until(() -> bean.acknowledged(DEFAULT_PROCESSING_ACK_PAYLOAD).size() == 5);
    assertThat(bean.acknowledged(DEFAULT_PROCESSING_ACK_PAYLOAD)).containsExactly("a", "b", "c", "d", "e");
    assertThat(bean.received(DEFAULT_PROCESSING_ACK_PAYLOAD)).containsExactly("a", "b", "c", "d", "e");
    assertThat(bean.acknowledgeTimeStamps(DEFAULT_PROCESSING_ACK_PAYLOAD).get(0)).isGreaterThan(bean.receivedTimeStamps(DEFAULT_PROCESSING_ACK_PAYLOAD).get(0));
  }



}
