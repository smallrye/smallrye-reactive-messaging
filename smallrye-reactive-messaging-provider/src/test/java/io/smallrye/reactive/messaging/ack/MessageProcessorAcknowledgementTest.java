package io.smallrye.reactive.messaging.ack;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static io.smallrye.reactive.messaging.ack.BeanWithMessageProcessors.*;

public class MessageProcessorAcknowledgementTest extends AcknowledgmentTestBase {

  private final Class<BeanWithMessageProcessors> beanClass = BeanWithMessageProcessors.class;

  @Before
  public void initialize() {
    acks = Arrays.asList("a", "b", "c", "d", "e");
    expected = Arrays.asList("a", "a", "b", "b", "c", "c", "d", "d", "e", "e");
  }

  @Test
  public void testManualAcknowledgement() {
    SpiedBeanHelper bean = installInitializeAndGet(beanClass);
    assertAcknowledgment(bean, MANUAL_ACKNOWLEDGMENT);
  }

  @Test
  public void testManualAcknowledgementBuilder() {
    SpiedBeanHelper bean = installInitializeAndGet(beanClass);
    assertAcknowledgment(bean, MANUAL_ACKNOWLEDGMENT_BUILDER);
  }

  @Test
  public void testNoAcknowledgement() {
    SpiedBeanHelper bean = installInitializeAndGet(beanClass);
    assertNoAcknowledgment(bean, NO_ACKNOWLEDGMENT);
  }

  @Test
  public void testNoAcknowledgementBuilder() {
    SpiedBeanHelper bean = installInitializeAndGet(beanClass);
    assertNoAcknowledgment(bean, NO_ACKNOWLEDGMENT_BUILDER);
  }

  @Test
  public void testPreAck() {
    SpiedBeanHelper bean = installInitializeAndGet(beanClass);
    assertPreAcknowledgment(bean, PRE_ACKNOWLEDGMENT);
  }

  @Test
  public void testPreAckBuilder() {
    SpiedBeanHelper bean = installInitializeAndGet(beanClass);
    assertPreAcknowledgment(bean, PRE_ACKNOWLEDGMENT_BUILDER);
  }

  @Test
  public void testDefaultAck() {
    SpiedBeanHelper bean = installInitializeAndGet(beanClass);
    assertPreAcknowledgment(bean, DEFAULT_ACKNOWLEDGMENT);
  }

  @Test
  public void testDefaultAckBuilder() {
    SpiedBeanHelper bean = installInitializeAndGet(beanClass);
    assertPreAcknowledgment(bean, DEFAULT_ACKNOWLEDGMENT_BUILDER);
  }


}
