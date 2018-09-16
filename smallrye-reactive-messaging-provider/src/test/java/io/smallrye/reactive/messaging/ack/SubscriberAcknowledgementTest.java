package io.smallrye.reactive.messaging.ack;

import org.junit.Test;

import static io.smallrye.reactive.messaging.ack.SubscriberBeanWithMethodsReturningSubscribers.*;

public class SubscriberAcknowledgementTest extends AcknowlegmentTestBase {

  private final Class<SubscriberBeanWithMethodsReturningSubscribers> beanClass = SubscriberBeanWithMethodsReturningSubscribers.class;

  @Test
  public void testManualAcknowledgementWithMethodReturningSubscriber() {
    SubscriberBeanWithMethodsReturningSubscribers bean = installInitializeAndGet(beanClass);
    assertAcknowledgment(bean, MANUAL_ACKNOWLEDGMENT_MESSAGE);
  }

  @Test
  public void testNoAcknowledgementMessageWithMethodReturningSubscriber() {
    SubscriberBeanWithMethodsReturningSubscribers bean = installInitializeAndGet(beanClass);
    assertNoAcknowledgment(bean, NO_ACKNOWLEDGMENT_MESSAGE);
  }

  @Test
  public void testNoAcknowledgementPayloadWithMethodReturningSubscriber() {
    SubscriberBeanWithMethodsReturningSubscribers bean = installInitializeAndGet(beanClass);
    assertNoAcknowledgment(bean, NO_ACKNOWLEDGMENT_PAYLOAD);
  }

  @Test
  public void testPreProcessingAcknowledgementMessageWithMethodReturningSubscriber() {
    SubscriberBeanWithMethodsReturningSubscribers bean = installInitializeAndGet(beanClass);
    assertPreAcknowledgment(bean, PRE_PROCESSING_ACK_MESSAGE);
  }

  @Test
  public void testPreProcessingAcknowledgementPayloadWithMethodReturningSubscriber() {
    SubscriberBeanWithMethodsReturningSubscribers bean = installInitializeAndGet(beanClass);
    assertPreAcknowledgment(bean, PRE_PROCESSING_ACK_PAYLOAD);
  }

  @Test
  public void testPostProcessingAcknowledgementMessageWithMethodReturningSubscriber() {
    SubscriberBeanWithMethodsReturningSubscribers bean = installInitializeAndGet(beanClass);
    assertPostAcknowledgment(bean, POST_PROCESSING_ACK_MESSAGE);
  }

  @Test
  public void testPostProcessingAcknowledgementPayloadWithMethodReturningSubscriber() {
    SubscriberBeanWithMethodsReturningSubscribers bean = installInitializeAndGet(beanClass);
    assertPostAcknowledgment(bean, POST_PROCESSING_ACK_PAYLOAD);
  }

  @Test
  public void testDefaultProcessingAcknowledgementMessageWithMethodReturningSubscriber() {
    SubscriberBeanWithMethodsReturningSubscribers bean = installInitializeAndGet(beanClass);
    assertPostAcknowledgment(bean, DEFAULT_PROCESSING_ACK_MESSAGE);
  }

  @Test
  public void testDefaultProcessingAcknowledgementPayloadMessageWithMethodReturningSubscriber() {
    SubscriberBeanWithMethodsReturningSubscribers bean = installInitializeAndGet(beanClass);
    assertPostAcknowledgment(bean, DEFAULT_PROCESSING_ACK_PAYLOAD);
  }

}
