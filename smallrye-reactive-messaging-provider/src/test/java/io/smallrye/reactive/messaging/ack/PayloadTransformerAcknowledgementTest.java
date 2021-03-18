package io.smallrye.reactive.messaging.ack;

import static io.smallrye.reactive.messaging.ack.BeanWithProcessorsManipulatingPayloads.DEFAULT_ACKNOWLEDGMENT;
import static io.smallrye.reactive.messaging.ack.BeanWithProcessorsManipulatingPayloads.DEFAULT_ACKNOWLEDGMENT_CS;
import static io.smallrye.reactive.messaging.ack.BeanWithProcessorsManipulatingPayloads.DEFAULT_ACKNOWLEDGMENT_UNI;
import static io.smallrye.reactive.messaging.ack.BeanWithProcessorsManipulatingPayloads.NO_ACKNOWLEDGMENT;
import static io.smallrye.reactive.messaging.ack.BeanWithProcessorsManipulatingPayloads.NO_ACKNOWLEDGMENT_CS;
import static io.smallrye.reactive.messaging.ack.BeanWithProcessorsManipulatingPayloads.NO_ACKNOWLEDGMENT_UNI;
import static io.smallrye.reactive.messaging.ack.BeanWithProcessorsManipulatingPayloads.POST_ACKNOWLEDGMENT;
import static io.smallrye.reactive.messaging.ack.BeanWithProcessorsManipulatingPayloads.POST_ACKNOWLEDGMENT_CS;
import static io.smallrye.reactive.messaging.ack.BeanWithProcessorsManipulatingPayloads.POST_ACKNOWLEDGMENT_UNI;
import static io.smallrye.reactive.messaging.ack.BeanWithProcessorsManipulatingPayloads.PRE_ACKNOWLEDGMENT;
import static io.smallrye.reactive.messaging.ack.BeanWithProcessorsManipulatingPayloads.PRE_ACKNOWLEDGMENT_CS;
import static io.smallrye.reactive.messaging.ack.BeanWithProcessorsManipulatingPayloads.PRE_ACKNOWLEDGMENT_UNI;

import org.junit.jupiter.api.Test;

public class PayloadTransformerAcknowledgementTest extends AcknowledgmentTestBase {

    private final Class<BeanWithProcessorsManipulatingPayloads> beanClass = BeanWithProcessorsManipulatingPayloads.class;

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
    public void testPostAckUni() {
        SpiedBeanHelper bean = installInitializeAndGet(beanClass);
        assertPostAcknowledgment(bean, POST_ACKNOWLEDGMENT_UNI);
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
