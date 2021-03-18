package io.smallrye.reactive.messaging.ack;

import static io.smallrye.reactive.messaging.ack.SubscriberBeanWithMethodsReturningUni.*;

import org.junit.jupiter.api.Test;

public class SubscriberWithUniMethodAcknowledgementTest extends AcknowledgmentTestBase {

    private final Class<SubscriberBeanWithMethodsReturningUni> beanClass = SubscriberBeanWithMethodsReturningUni.class;

    @Test
    public void test() {
        SubscriberBeanWithMethodsReturningUni bean = installInitializeAndGet(beanClass);
        testManual(bean);
        testNoAcknowledgementMessage(bean);
        testNoAcknowledgementPayload(bean);
        testPreProcessingAcknowledgementMessage(bean);
        testPreProcessingAcknowledgementPayload(bean);
        testPostProcessingAcknowledgementMessage(bean);
        testPostProcessingAcknowledgementPayload(bean);
        testDefaultProcessingAcknowledgementMessage(bean);
        testDefaultProcessingAcknowledgementPayload(bean);
    }

    public void testManual(SpiedBeanHelper bean) {
        assertAcknowledgment(bean, MANUAL_ACKNOWLEDGMENT);
    }

    public void testNoAcknowledgementMessage(SpiedBeanHelper bean) {
        assertNoAcknowledgment(bean, NO_ACKNOWLEDGMENT_MESSAGE);
    }

    public void testNoAcknowledgementPayload(SpiedBeanHelper bean) {
        assertNoAcknowledgment(bean, NO_ACKNOWLEDGMENT_PAYLOAD);
    }

    public void testPreProcessingAcknowledgementMessage(SpiedBeanHelper bean) {
        assertPreAcknowledgment(bean, PRE_PROCESSING_ACKNOWLEDGMENT_MESSAGE);
    }

    public void testPreProcessingAcknowledgementPayload(SpiedBeanHelper bean) {
        assertPreAcknowledgment(bean, PRE_PROCESSING_ACKNOWLEDGMENT_PAYLOAD);
    }

    public void testPostProcessingAcknowledgementMessage(SpiedBeanHelper bean) {
        assertPostAcknowledgment(bean, POST_PROCESSING_ACKNOWLEDGMENT_MESSAGE);
    }

    public void testPostProcessingAcknowledgementPayload(SpiedBeanHelper bean) {
        assertPostAcknowledgment(bean, POST_PROCESSING_ACKNOWLEDGMENT_PAYLOAD);
    }

    public void testDefaultProcessingAcknowledgementMessage(SpiedBeanHelper bean) {
        assertPostAcknowledgment(bean, DEFAULT_PROCESSING_ACKNOWLEDGMENT_MESSAGE);
    }

    public void testDefaultProcessingAcknowledgementPayload(SpiedBeanHelper bean) {
        assertPostAcknowledgment(bean, DEFAULT_PROCESSING_ACKNOWLEDGMENT_PAYLOAD);
    }

}
