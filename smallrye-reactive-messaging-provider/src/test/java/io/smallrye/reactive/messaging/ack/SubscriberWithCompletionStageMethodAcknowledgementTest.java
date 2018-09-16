package io.smallrye.reactive.messaging.ack;

import org.junit.Test;

import static io.smallrye.reactive.messaging.ack.SubscriberBeanWithMethodsReturningCompletionStage.*;

public class SubscriberWithCompletionStageMethodAcknowledgementTest extends AcknowlegmentTestBase {

  private final Class<SubscriberBeanWithMethodsReturningCompletionStage> beanClass = SubscriberBeanWithMethodsReturningCompletionStage.class;

  @Test
  public void testManual() {
    SubscriberBeanWithMethodsReturningCompletionStage bean = installInitializeAndGet(beanClass);
    assertAcknowledgment(bean, MANUAL_ACKNOWLEDGMENT);
  }

  @Test
  public void testNoAcknowledgementMessage() {
    SubscriberBeanWithMethodsReturningCompletionStage bean = installInitializeAndGet(beanClass);
    assertNoAcknowledgment(bean, NO_ACKNOWLEDGMENT_MESSAGE);
  }

  @Test
  public void testNoAcknowledgementPayload() {
    SubscriberBeanWithMethodsReturningCompletionStage bean = installInitializeAndGet(beanClass);
    assertNoAcknowledgment(bean, NO_ACKNOWLEDGMENT_PAYLOAD);
  }

  @Test
  public void testPreProcessingAcknowledgementMessage() {
    SubscriberBeanWithMethodsReturningCompletionStage bean = installInitializeAndGet(beanClass);
    assertPreAcknowledgment(bean, PRE_PROCESSING_ACKNOWLEDGMENT_MESSAGE);
  }

  @Test
  public void testPreProcessingAcknowledgementPayload() {
    SubscriberBeanWithMethodsReturningCompletionStage bean = installInitializeAndGet(beanClass);
    assertPreAcknowledgment(bean, PRE_PROCESSING_ACKNOWLEDGMENT_PAYLOAD);
  }

  @Test
  public void testPostProcessingAcknowledgementMessage() {
    SubscriberBeanWithMethodsReturningCompletionStage bean = installInitializeAndGet(beanClass);
    assertPostAcknowledgment(bean, POST_PROCESSING_ACKNOWLEDGMENT_MESSAGE);
  }

  @Test
  public void testPostProcessingAcknowledgementPayload() {
    SubscriberBeanWithMethodsReturningCompletionStage bean = installInitializeAndGet(beanClass);
    assertPostAcknowledgment(bean, POST_PROCESSING_ACKNOWLEDGMENT_PAYLOAD);
  }

  @Test
  public void testDefaultProcessingAcknowledgementMessage() {
    SubscriberBeanWithMethodsReturningCompletionStage bean = installInitializeAndGet(beanClass);
    assertPostAcknowledgment(bean, DEFAULT_PROCESSING_ACKNOWLEDGMENT_MESSAGE);
  }

  @Test
  public void testDefaultProcessingAcknowledgementPayload() {
    SubscriberBeanWithMethodsReturningCompletionStage bean = installInitializeAndGet(beanClass);
    assertPostAcknowledgment(bean, DEFAULT_PROCESSING_ACKNOWLEDGMENT_PAYLOAD);
  }



}
