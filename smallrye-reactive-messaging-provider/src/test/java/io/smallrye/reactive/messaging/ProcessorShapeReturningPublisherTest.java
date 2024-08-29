package io.smallrye.reactive.messaging;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.beans.BeanProducingAPublisherBuilderOfMessagesAndConsumingIndividualMessage;
import io.smallrye.reactive.messaging.beans.BeanProducingAPublisherBuilderOfPayloadsAndConsumingIndividualPayload;
import io.smallrye.reactive.messaging.beans.BeanProducingAPublisherBuilderOfPayloadsAndConsumingIndividualPayloadWithProcessingError;
import io.smallrye.reactive.messaging.beans.BeanProducingAPublisherOfMessagesAndConsumingIndividualMessage;
import io.smallrye.reactive.messaging.beans.BeanProducingAPublisherOfPayloadsAndConsumingIndividualPayload;
import io.smallrye.reactive.messaging.beans.BeanProducingAPublisherOfPayloadsAndConsumingIndividualPayloadWithProcessingError;

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
    public void BeanProducingAPublisherOfPayloadsAndConsumingIndividualPayloadWithProcessingError() {
        addBeanClass(BeanProducingAPublisherOfPayloadsAndConsumingIndividualPayloadWithProcessingError.class);
        initialize();
        MyCollector collector = container.select(MyCollector.class).get();
        assertThat(collector.payloads()).isEqualTo(IntStream.rangeClosed(1, 5)
                .map(i -> i * 2)
                .flatMap(i -> IntStream.of(i, i)).boxed()
                .map(Object::toString).collect(Collectors.toList()));
    }

    @Test
    public void testBeanProducingAPublisherBuilderOfPayloadsAndConsumingIndividualPayload() {
        addBeanClass(BeanProducingAPublisherBuilderOfPayloadsAndConsumingIndividualPayload.class);
        initialize();
        MyCollector collector = container.select(MyCollector.class).get();
        assertThat(collector.payloads()).isEqualTo(EXPECTED);
    }

    @Test
    public void testBeanProducingAPublisherBuilderOfPayloadsAndConsumingIndividualPayloadWithProcessingError() {
        addBeanClass(BeanProducingAPublisherBuilderOfPayloadsAndConsumingIndividualPayloadWithProcessingError.class);
        initialize();
        MyCollector collector = container.select(MyCollector.class).get();
        assertThat(collector.payloads()).isEqualTo(IntStream.rangeClosed(1, 6).flatMap(i -> IntStream.of(i, i)).boxed()
                .map(Object::toString).collect(Collectors.toList()));
    }

    @Test
    public void testBeanProducingAPublisherBuilderOfMessagesAndConsumingIndividualMessage() {
        addBeanClass(BeanProducingAPublisherBuilderOfMessagesAndConsumingIndividualMessage.class);
        initialize();
        MyCollector collector = container.select(MyCollector.class).get();
        assertThat(collector.payloads()).isEqualTo(EXPECTED);
    }

}
