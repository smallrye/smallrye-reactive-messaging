package io.smallrye.reactive.messaging.ack;

import org.junit.Test;

import static io.smallrye.reactive.messaging.ack.BeanWithProcessorsManipulatingMessages.*;

public class MessageTransformerAcknowledgementTest extends AcknowledgmentTestBase {

  private final Class<BeanWithProcessorsManipulatingMessages> beanClass = BeanWithProcessorsManipulatingMessages.class;

  @Test
  public void testManualCSAcknowledgement() {
    SpiedBeanHelper bean = installInitializeAndGet(beanClass);
    assertAcknowledgment(bean, MANUAL_ACKNOWLEDGMENT_CS);
  }

  @Test
  public void testNoAcknowledgement() {
    SpiedBeanHelper bean = installInitializeAndGet(beanClass);
    assertNoAcknowledgment(bean, NO_ACKNOWLEDGMENT);
  }

  @Test
  public void testNoAcknowledgementCS() {
    SpiedBeanHelper bean = installInitializeAndGet(beanClass);
    assertNoAcknowledgment(bean, NO_ACKNOWLEDGMENT_CS);
  }

  @Test
  public void testPreAck() {
    SpiedBeanHelper bean = installInitializeAndGet(beanClass);
    assertPreAcknowledgment(bean, PRE_ACKNOWLEDGMENT);
  }

  @Test
  public void testPreAckCS() {
    SpiedBeanHelper bean = installInitializeAndGet(beanClass);
    assertPreAcknowledgment(bean, PRE_ACKNOWLEDGMENT_CS);
  }

  @Test
  public void testPostAck() {
    SpiedBeanHelper bean = installInitializeAndGet(beanClass);
    assertPostAcknowledgment(bean, POST_ACKNOWLEDGMENT);
  }

  @Test
  public void testPostAckCS() {
    SpiedBeanHelper bean = installInitializeAndGet(beanClass);
    assertPostAcknowledgment(bean, POST_ACKNOWLEDGMENT_CS);
  }

  @Test
  public void testDefaultAck() {
    SpiedBeanHelper bean = installInitializeAndGet(beanClass);
    assertPostAcknowledgment(bean, DEFAULT_ACKNOWLEDGMENT);
  }

  @Test
  public void testDefaultAckCS() {
    SpiedBeanHelper bean = installInitializeAndGet(beanClass);
    assertPostAcknowledgment(bean, DEFAULT_ACKNOWLEDGMENT_CS);
  }


}
