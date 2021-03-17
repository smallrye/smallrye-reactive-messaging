package io.smallrye.reactive.messaging.ack;

import static io.smallrye.reactive.messaging.ack.SubscriberBeanWithMethodsReturningVoid.DEFAULT_ACKNOWLEDGMENT;
import static io.smallrye.reactive.messaging.ack.SubscriberBeanWithMethodsReturningVoid.MANUAL_ACKNOWLEDGMENT;
import static io.smallrye.reactive.messaging.ack.SubscriberBeanWithMethodsReturningVoid.NO_ACKNOWLEDGMENT;
import static io.smallrye.reactive.messaging.ack.SubscriberBeanWithMethodsReturningVoid.POST_PROCESSING_ACKNOWLEDGMENT;
import static io.smallrye.reactive.messaging.ack.SubscriberBeanWithMethodsReturningVoid.PRE_PROCESSING_ACKNOWLEDGMENT;

import org.junit.jupiter.api.Test;

public class SubscriberWithVoidMethodAcknowledgementTest extends AcknowledgmentTestBase {

    @Test
    public void testManualAcknowledgement() {
        SubscriberBeanWithMethodsReturningVoid bean = installInitializeAndGet(SubscriberBeanWithMethodsReturningVoid.class);
        assertAcknowledgment(bean, MANUAL_ACKNOWLEDGMENT);
    }

    @Test
    public void testNoAcknowledgement() {
        SubscriberBeanWithMethodsReturningVoid bean = installInitializeAndGet(SubscriberBeanWithMethodsReturningVoid.class);
        assertNoAcknowledgment(bean, NO_ACKNOWLEDGMENT);
    }

    @Test
    public void testDefaultAcknowledgement() {
        SubscriberBeanWithMethodsReturningVoid bean = installInitializeAndGet(SubscriberBeanWithMethodsReturningVoid.class);
        assertPostAcknowledgment(bean, DEFAULT_ACKNOWLEDGMENT);
    }

    @Test
    public void testPostAcknowledgement() {
        SubscriberBeanWithMethodsReturningVoid bean = installInitializeAndGet(SubscriberBeanWithMethodsReturningVoid.class);
        assertPostAcknowledgment(bean, POST_PROCESSING_ACKNOWLEDGMENT);
    }

    @Test
    public void testPreAcknowledgement() {
        SubscriberBeanWithMethodsReturningVoid bean = installInitializeAndGet(SubscriberBeanWithMethodsReturningVoid.class);
        assertPreAcknowledgment(bean, PRE_PROCESSING_ACKNOWLEDGMENT);
    }

}
