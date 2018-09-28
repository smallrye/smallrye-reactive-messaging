package io.smallrye.reactive.messaging.ack;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static io.smallrye.reactive.messaging.ack.BeanWithStreamTransformers.*;

public class StreamTransformerAcknowledgementTest extends AcknowledgmentTestBase {

  private final Class<BeanWithStreamTransformers> beanClass = BeanWithStreamTransformers.class;


  @Before
  public void configure() {
    acks = Arrays.asList("a", "b", "c", "d", "e");
    expected = Arrays.asList("a", "a", "b", "b", "c", "c", "d", "d", "e", "e");
  }

  @Test
  public void testManualAck() {
    SpiedBeanHelper bean = installInitializeAndGet(beanClass);
    assertAcknowledgment(bean, MANUAL_ACKNOWLEDGMENT);
  }

  @Test
  public void testManualAckBuilder() {
    SpiedBeanHelper bean = installInitializeAndGet(beanClass);
    assertAcknowledgment(bean, MANUAL_ACKNOWLEDGMENT_BUILDER);
  }

  @Test
  public void testNoAck() {
    SpiedBeanHelper bean = installInitializeAndGet(beanClass);
    assertNoAcknowledgment(bean, NO_ACKNOWLEDGMENT);
  }

  @Test
  public void testNoAckBuilder() {
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

  @Test
  public void testPayloadNoAck() {
    SpiedBeanHelper bean = installInitializeAndGet(beanClass);
    assertNoAcknowledgment(bean, PAYLOAD_NO_ACKNOWLEDGMENT);
  }

  @Test
  public void testPayloadNoAckBuilder() {
    SpiedBeanHelper bean = installInitializeAndGet(beanClass);
    assertNoAcknowledgment(bean, PAYLOAD_NO_ACKNOWLEDGMENT_BUILDER);
  }

  @Test
  public void testPayloadPreAck() {
    SpiedBeanHelper bean = installInitializeAndGet(beanClass);
    assertPreAcknowledgment(bean, PAYLOAD_PRE_ACKNOWLEDGMENT);
  }

  @Test
  public void testPayloadPreAckBuilder() {
    SpiedBeanHelper bean = installInitializeAndGet(beanClass);
    assertPreAcknowledgment(bean, PAYLOAD_PRE_ACKNOWLEDGMENT_BUILDER);
  }

  @Test
  public void testPayloadDefaultAck() {
    SpiedBeanHelper bean = installInitializeAndGet(beanClass);
    assertPreAcknowledgment(bean, PAYLOAD_PRE_ACKNOWLEDGMENT);
  }

  @Test
  public void testPayloadDefaultAckBuilder() {
    SpiedBeanHelper bean = installInitializeAndGet(beanClass);
    assertPreAcknowledgment(bean, PAYLOAD_PRE_ACKNOWLEDGMENT_BUILDER);
  }


  //TODO Test with and without message ?

  // TODO Default
  // TODO Post?

}
