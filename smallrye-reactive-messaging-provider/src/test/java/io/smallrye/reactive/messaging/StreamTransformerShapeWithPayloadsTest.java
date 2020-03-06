package io.smallrye.reactive.messaging;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import io.smallrye.reactive.messaging.beans.*;

public class StreamTransformerShapeWithPayloadsTest extends WeldTestBase {

    @Test
    public void testBeanConsumingItemAsFlowableAndPublishingItemAsFlowable() {
        addBeanClass(BeanConsumingItemAsFlowableAndPublishingItemAsFlowable.class);
        initialize();
        MyCollector collector = container.getBeanManager().createInstance().select(MyCollector.class).get();
        assertThat(collector.payloads()).isEqualTo(EXPECTED);
    }

    @Test
    public void testBeanConsumingItemAsMultiAndPublishingItemAsMulti() {
        addBeanClass(BeanConsumingItemAsMultiAndPublishingItemAsMulti.class);
        initialize();
        MyCollector collector = container.getBeanManager().createInstance().select(MyCollector.class).get();
        assertThat(collector.payloads()).isEqualTo(EXPECTED);
    }

    @Test
    public void testBeanConsumingItemAsFluxAndPublishingItemAsFlux() {
        addBeanClass(BeanConsumingItemAsFluxAndPublishingItemAsFlux.class);
        initialize();
        MyCollector collector = container.getBeanManager().createInstance().select(MyCollector.class).get();
        assertThat(collector.payloads()).isEqualTo(EXPECTED);
    }

    @Test
    public void testBeanConsumingItemAsFlowableAndPublishingItemAsPublisher() {
        addBeanClass(BeanConsumingItemAsFlowableAndPublishingItemAsPublisher.class);
        initialize();
        MyCollector collector = container.getBeanManager().createInstance().select(MyCollector.class).get();
        assertThat(collector.payloads()).isEqualTo(EXPECTED);
    }

    @Test
    public void testBeanConsumingItemAsMultiAndPublishingItemAsPublisher() {
        addBeanClass(BeanConsumingItemAsMultiAndPublishingItemAsPublisher.class);
        initialize();
        MyCollector collector = container.getBeanManager().createInstance().select(MyCollector.class).get();
        assertThat(collector.payloads()).isEqualTo(EXPECTED);
    }

    @Test
    public void testBeanConsumingItemAsFluxAndPublishingItemAsPublisher() {
        addBeanClass(BeanConsumingItemAsFluxAndPublishingItemAsPublisher.class);
        initialize();
        MyCollector collector = container.getBeanManager().createInstance().select(MyCollector.class).get();
        assertThat(collector.payloads()).isEqualTo(EXPECTED);
    }

    @Test
    public void testBeanConsumingItemAsPublisherAndPublishingItemAsFlowable() {
        addBeanClass(BeanConsumingItemAsPublisherAndPublishingItemAsFlowable.class);
        initialize();
        MyCollector collector = container.getBeanManager().createInstance().select(MyCollector.class).get();
        assertThat(collector.payloads()).isEqualTo(EXPECTED);
    }

    @Test
    public void testBeanConsumingItemAsPublisherAndPublishingItemAsMulti() {
        addBeanClass(BeanConsumingItemAsPublisherAndPublishingItemAsMulti.class);
        initialize();
        MyCollector collector = container.getBeanManager().createInstance().select(MyCollector.class).get();
        assertThat(collector.payloads()).isEqualTo(EXPECTED);
    }

    @Test
    public void testBeanConsumingItemAsPublisherBuilderAndPublishingItemAsPublisherBuilder() {
        addBeanClass(BeanConsumingItemAsPublisherBuilderAndPublishingItemAsPublisherBuilder.class);
        initialize();
        MyCollector collector = container.getBeanManager().createInstance().select(MyCollector.class).get();
        assertThat(collector.payloads()).isEqualTo(EXPECTED);
    }

}
