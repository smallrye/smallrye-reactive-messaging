package io.smallrye.reactive.messaging;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.beans.BeanProducingAPublisherBuilderOfMessagesAndConsumingIndividualMessage;
import io.smallrye.reactive.messaging.beans.BeanProducingAPublisherBuilderOfPayloadsAndConsumingIndividualPayload;
import io.smallrye.reactive.messaging.beans.BeanProducingAPublisherOfMessagesAndConsumingIndividualMessage;
import io.smallrye.reactive.messaging.beans.BeanProducingAPublisherOfPayloadsAndConsumingIndividualPayload;

public class ProcessorShapeReturningPublisherTest extends WeldTestBase {

    @Test
    public void testBeanProducingAPublisherOfMessagesAndConsumingIndividualMessage() {
        addBeanClass(BeanProducingAPublisherOfMessagesAndConsumingIndividualMessage.class);
        initialize();
        MyCollector collector = container.select(MyCollector.class).get();
        assertThat(collector.payloads()).isEqualTo(EXPECTED);
    }

    @Test
    public void testBeanProducingAPublisherOfPayloadsAndConsumingIndividualPayload() {
        addBeanClass(BeanProducingAPublisherOfPayloadsAndConsumingIndividualPayload.class);
        initialize();
        MyCollector collector = container.select(MyCollector.class).get();
        assertThat(collector.payloads()).isEqualTo(EXPECTED);
    }

    @Test
    public void testBeanProducingAPublisherBuilderOfPayloadsAndConsumingIndividualPayload() {
        addBeanClass(BeanProducingAPublisherBuilderOfPayloadsAndConsumingIndividualPayload.class);
        initialize();
        MyCollector collector = container.select(MyCollector.class).get();
        assertThat(collector.payloads()).isEqualTo(EXPECTED);
    }

    @Test
    public void testBeanProducingAPublisherBuilderOfMessagesAndConsumingIndividualMessage() {
        addBeanClass(BeanProducingAPublisherBuilderOfMessagesAndConsumingIndividualMessage.class);
        initialize();
        MyCollector collector = container.select(MyCollector.class).get();
        assertThat(collector.payloads()).isEqualTo(EXPECTED);
    }

}
