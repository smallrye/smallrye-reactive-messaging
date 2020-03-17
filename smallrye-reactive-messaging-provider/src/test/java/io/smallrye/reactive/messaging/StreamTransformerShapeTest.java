package io.smallrye.reactive.messaging;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import io.smallrye.reactive.messaging.beans.*;

public class StreamTransformerShapeTest extends WeldTestBase {

    @Test
    public void testBeanConsumingMsgAsFlowableAndPublishingMsgAsFlowable() {
        addBeanClass(BeanConsumingMsgAsFlowableAndPublishingMsgAsFlowable.class);
        initialize();
        MyCollector collector = container.select(MyCollector.class).get();
        assertThat(collector.payloads()).isEqualTo(EXPECTED);
    }

    @Test
    public void testBeanConsumingMsgAsMultiAndPublishingMsgAsMulti() {
        addBeanClass(BeanConsumingMsgAsMultiAndPublishingMsgAsMulti.class);
        initialize();
        MyCollector collector = container.select(MyCollector.class).get();
        assertThat(collector.payloads()).isEqualTo(EXPECTED);
    }

    @Test
    public void testBeanConsumingMsgAsFluxAndPublishingMsgAsFlux() {
        addBeanClass(BeanConsumingMsgAsFluxAndPublishingMsgAsFlux.class);
        initialize();
        MyCollector collector = container.select(MyCollector.class).get();
        assertThat(collector.payloads()).isEqualTo(EXPECTED);
    }

    @Test
    public void testBeanConsumingMsgAsFlowableAndPublishingMsgAsPublisher() {
        addBeanClass(BeanConsumingMsgAsFlowableAndPublishingMsgAsPublisher.class);
        initialize();
        MyCollector collector = container.select(MyCollector.class).get();
        assertThat(collector.payloads()).isEqualTo(EXPECTED);
    }

    @Test
    public void testBeanConsumingMsgAsMultiAndPublishingMsgAsPublisher() {
        addBeanClass(BeanConsumingMsgAsMultiAndPublishingMsgAsPublisher.class);
        initialize();
        MyCollector collector = container.select(MyCollector.class).get();
        assertThat(collector.payloads()).isEqualTo(EXPECTED);
    }

    @Test
    public void testBeanConsumingMsgAsFluxAndPublishingMsgAsPublisher() {
        addBeanClass(BeanConsumingMsgAsFluxAndPublishingMsgAsPublisher.class);
        initialize();
        MyCollector collector = container.select(MyCollector.class).get();
        assertThat(collector.payloads()).isEqualTo(EXPECTED);
    }

    @Test
    public void testBeanConsumingMsgAsPublisherAndPublishingMsgAsFlowable() {
        addBeanClass(BeanConsumingMsgAsPublisherAndPublishingMsgAsFlowable.class);
        initialize();
        MyCollector collector = container.select(MyCollector.class).get();
        assertThat(collector.payloads()).isEqualTo(EXPECTED);
    }

    @Test
    public void testBeanConsumingMsgAsPublisherAndPublishingMsgAsMulti() {
        addBeanClass(BeanConsumingMsgAsPublisherAndPublishingMsgAsMulti.class);
        initialize();
        MyCollector collector = container.select(MyCollector.class).get();
        assertThat(collector.payloads()).isEqualTo(EXPECTED);
    }

    @Test
    public void testBeanConsumingMsgAsPublisherBuilderAndPublishingMsgAsPublisherBuilder() {
        addBeanClass(BeanConsumingMsgAsPublisherBuilderAndPublishingMsgAsPublisherBuilder.class);
        initialize();
        MyCollector collector = container.select(MyCollector.class).get();
        assertThat(collector.payloads()).isEqualTo(EXPECTED);
    }

    @Test
    public void testBeanProducingAProcessor() {
        addBeanClass(BeanProducingAProcessorOfMessages.class);
        initialize();
        MyCollector collector = container.select(MyCollector.class).get();
        assertThat(collector.payloads()).isEqualTo(EXPECTED);
    }

    @Test
    public void testBeanProducingAProcessorBuilder() {
        addBeanClass(BeanProducingAProcessorBuilderOfMessages.class);
        initialize();
        MyCollector collector = container.select(MyCollector.class).get();
        assertThat(collector.payloads()).isEqualTo(EXPECTED);
    }

}
