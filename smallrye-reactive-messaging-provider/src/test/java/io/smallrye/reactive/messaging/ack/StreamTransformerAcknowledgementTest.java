package io.smallrye.reactive.messaging.ack;

import static io.smallrye.reactive.messaging.ack.BeanWithStreamTransformers.DEFAULT_ACKNOWLEDGMENT;
import static io.smallrye.reactive.messaging.ack.BeanWithStreamTransformers.DEFAULT_ACKNOWLEDGMENT_BUILDER;
import static io.smallrye.reactive.messaging.ack.BeanWithStreamTransformers.MANUAL_ACKNOWLEDGMENT;
import static io.smallrye.reactive.messaging.ack.BeanWithStreamTransformers.MANUAL_ACKNOWLEDGMENT_BUILDER;
import static io.smallrye.reactive.messaging.ack.BeanWithStreamTransformers.NO_ACKNOWLEDGMENT;
import static io.smallrye.reactive.messaging.ack.BeanWithStreamTransformers.NO_ACKNOWLEDGMENT_BUILDER;
import static io.smallrye.reactive.messaging.ack.BeanWithStreamTransformers.PAYLOAD_NO_ACKNOWLEDGMENT;
import static io.smallrye.reactive.messaging.ack.BeanWithStreamTransformers.PAYLOAD_NO_ACKNOWLEDGMENT_BUILDER;
import static io.smallrye.reactive.messaging.ack.BeanWithStreamTransformers.PAYLOAD_PRE_ACKNOWLEDGMENT;
import static io.smallrye.reactive.messaging.ack.BeanWithStreamTransformers.PAYLOAD_PRE_ACKNOWLEDGMENT_BUILDER;
import static io.smallrye.reactive.messaging.ack.BeanWithStreamTransformers.PRE_ACKNOWLEDGMENT;
import static io.smallrye.reactive.messaging.ack.BeanWithStreamTransformers.PRE_ACKNOWLEDGMENT_BUILDER;

import java.util.Arrays;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class StreamTransformerAcknowledgementTest extends AcknowledgmentTestBase {

    private final Class<BeanWithStreamTransformers> beanClass = BeanWithStreamTransformers.class;

    @BeforeEach
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
        assertPostAcknowledgment(bean, DEFAULT_ACKNOWLEDGMENT);
    }

    @Test
    public void testDefaultAckBuilder() {
        SpiedBeanHelper bean = installInitializeAndGet(beanClass);
        assertPostAcknowledgment(bean, DEFAULT_ACKNOWLEDGMENT_BUILDER);
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

}
