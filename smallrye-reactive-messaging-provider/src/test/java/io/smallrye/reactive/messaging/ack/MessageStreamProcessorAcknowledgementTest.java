package io.smallrye.reactive.messaging.ack;

import static io.smallrye.reactive.messaging.ack.BeanWithProcessorsProducingMessageStreams.DEFAULT_ACKNOWLEDGMENT;
import static io.smallrye.reactive.messaging.ack.BeanWithProcessorsProducingMessageStreams.DEFAULT_ACKNOWLEDGMENT_BUILDER;
import static io.smallrye.reactive.messaging.ack.BeanWithProcessorsProducingMessageStreams.MANUAL_ACKNOWLEDGMENT;
import static io.smallrye.reactive.messaging.ack.BeanWithProcessorsProducingMessageStreams.MANUAL_ACKNOWLEDGMENT_BUILDER;
import static io.smallrye.reactive.messaging.ack.BeanWithProcessorsProducingMessageStreams.NO_ACKNOWLEDGMENT;
import static io.smallrye.reactive.messaging.ack.BeanWithProcessorsProducingMessageStreams.NO_ACKNOWLEDGMENT_BUILDER;
import static io.smallrye.reactive.messaging.ack.BeanWithProcessorsProducingMessageStreams.PRE_ACKNOWLEDGMENT;
import static io.smallrye.reactive.messaging.ack.BeanWithProcessorsProducingMessageStreams.PRE_ACKNOWLEDGMENT_BUILDER;

import java.util.Arrays;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class MessageStreamProcessorAcknowledgementTest extends AcknowledgmentTestBase {

    private final Class<BeanWithProcessorsProducingMessageStreams> beanClass = BeanWithProcessorsProducingMessageStreams.class;

    @BeforeEach
    public void configure() {
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
        assertPostAcknowledgment(bean, DEFAULT_ACKNOWLEDGMENT);
    }

    @Test
    public void testDefaultAckBuilder() {
        SpiedBeanHelper bean = installInitializeAndGet(beanClass);
        assertPostAcknowledgment(bean, DEFAULT_ACKNOWLEDGMENT_BUILDER);
    }

}
