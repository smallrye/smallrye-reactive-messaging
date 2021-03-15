package io.smallrye.reactive.messaging.ack;

import static io.smallrye.reactive.messaging.ack.BeanWithProcessorsManipulatingMessages.DEFAULT_ACKNOWLEDGMENT;
import static io.smallrye.reactive.messaging.ack.BeanWithProcessorsManipulatingMessages.DEFAULT_ACKNOWLEDGMENT_CS;
import static io.smallrye.reactive.messaging.ack.BeanWithProcessorsManipulatingMessages.DEFAULT_ACKNOWLEDGMENT_UNI;
import static io.smallrye.reactive.messaging.ack.BeanWithProcessorsManipulatingMessages.MANUAL_ACKNOWLEDGMENT_CS;
import static io.smallrye.reactive.messaging.ack.BeanWithProcessorsManipulatingMessages.MANUAL_ACKNOWLEDGMENT_UNI;
import static io.smallrye.reactive.messaging.ack.BeanWithProcessorsManipulatingMessages.NO_ACKNOWLEDGMENT;
import static io.smallrye.reactive.messaging.ack.BeanWithProcessorsManipulatingMessages.NO_ACKNOWLEDGMENT_CS;
import static io.smallrye.reactive.messaging.ack.BeanWithProcessorsManipulatingMessages.NO_ACKNOWLEDGMENT_UNI;
import static io.smallrye.reactive.messaging.ack.BeanWithProcessorsManipulatingMessages.PRE_ACKNOWLEDGMENT;
import static io.smallrye.reactive.messaging.ack.BeanWithProcessorsManipulatingMessages.PRE_ACKNOWLEDGMENT_CS;
import static io.smallrye.reactive.messaging.ack.BeanWithProcessorsManipulatingMessages.PRE_ACKNOWLEDGMENT_UNI;

import org.junit.jupiter.api.Test;

public class MessageTransformerAcknowledgementTest extends AcknowledgmentTestBase {

    private final Class<BeanWithProcessorsManipulatingMessages> beanClass = BeanWithProcessorsManipulatingMessages.class;

    @Test
    public void testManualCSAcknowledgement() {
        SpiedBeanHelper bean = installInitializeAndGet(beanClass);
        assertAcknowledgment(bean, MANUAL_ACKNOWLEDGMENT_CS);
    }

    @Test
    public void testManualUniAcknowledgement() {
        SpiedBeanHelper bean = installInitializeAndGet(beanClass);
        assertAcknowledgment(bean, MANUAL_ACKNOWLEDGMENT_UNI);
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
    public void testNoAcknowledgementUni() {
        SpiedBeanHelper bean = installInitializeAndGet(beanClass);
        assertNoAcknowledgment(bean, NO_ACKNOWLEDGMENT_UNI);
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
    public void testPreAckUni() {
        SpiedBeanHelper bean = installInitializeAndGet(beanClass);
        assertPreAcknowledgment(bean, PRE_ACKNOWLEDGMENT_UNI);
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

    @Test
    public void testDefaultAckUni() {
        SpiedBeanHelper bean = installInitializeAndGet(beanClass);
        assertPostAcknowledgment(bean, DEFAULT_ACKNOWLEDGMENT_UNI);
    }

}
