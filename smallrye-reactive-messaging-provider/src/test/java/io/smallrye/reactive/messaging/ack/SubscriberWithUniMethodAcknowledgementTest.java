package io.smallrye.reactive.messaging.ack;

import static io.smallrye.reactive.messaging.ack.SubscriberBeanWithMethodsReturningUni.*;

import org.junit.Test;

public class SubscriberWithUniMethodAcknowledgementTest extends AcknowledgmentTestBase {

    private final Class<SubscriberBeanWithMethodsReturningUni> beanClass = SubscriberBeanWithMethodsReturningUni.class;

    @Test
    public void testManual() {
        SubscriberBeanWithMethodsReturningUni bean = installInitializeAndGet(beanClass);
        assertAcknowledgment(bean, MANUAL_ACKNOWLEDGMENT);
    }

    @Test
    public void testNoAcknowledgementMessage() {
        SubscriberBeanWithMethodsReturningUni bean = installInitializeAndGet(beanClass);
        assertNoAcknowledgment(bean, NO_ACKNOWLEDGMENT_MESSAGE);
    }

    @Test
    public void testNoAcknowledgementPayload() {
        SubscriberBeanWithMethodsReturningUni bean = installInitializeAndGet(beanClass);
        assertNoAcknowledgment(bean, NO_ACKNOWLEDGMENT_PAYLOAD);
    }

    @Test
    public void testPreProcessingAcknowledgementMessage() {
        SubscriberBeanWithMethodsReturningUni bean = installInitializeAndGet(beanClass);
        assertPreAcknowledgment(bean, PRE_PROCESSING_ACKNOWLEDGMENT_MESSAGE);
    }

    @Test
    public void testPreProcessingAcknowledgementPayload() {
        SubscriberBeanWithMethodsReturningUni bean = installInitializeAndGet(beanClass);
        assertPreAcknowledgment(bean, PRE_PROCESSING_ACKNOWLEDGMENT_PAYLOAD);
    }

    @Test
    public void testPostProcessingAcknowledgementMessage() {
        SubscriberBeanWithMethodsReturningUni bean = installInitializeAndGet(beanClass);
        assertPostAcknowledgment(bean, POST_PROCESSING_ACKNOWLEDGMENT_MESSAGE);
    }

    @Test
    public void testPostProcessingAcknowledgementPayload() {
        SubscriberBeanWithMethodsReturningUni bean = installInitializeAndGet(beanClass);
        assertPostAcknowledgment(bean, POST_PROCESSING_ACKNOWLEDGMENT_PAYLOAD);
    }

    @Test
    public void testDefaultProcessingAcknowledgementMessage() {
        SubscriberBeanWithMethodsReturningUni bean = installInitializeAndGet(beanClass);
        assertPostAcknowledgment(bean, DEFAULT_PROCESSING_ACKNOWLEDGMENT_MESSAGE);
    }

    @Test
    public void testDefaultProcessingAcknowledgementPayload() {
        SubscriberBeanWithMethodsReturningUni bean = installInitializeAndGet(beanClass);
        assertPostAcknowledgment(bean, DEFAULT_PROCESSING_ACKNOWLEDGMENT_PAYLOAD);
    }

}
