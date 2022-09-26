package io.smallrye.reactive.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import jakarta.enterprise.inject.spi.DeploymentException;

import org.junit.jupiter.api.Test;

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
        assertThatThrownBy(this::initialize).isInstanceOf(DeploymentException.class);
    }

    @Test
    public void testBeanConsumingItemAsFlowableAndPublishingItemAsRSPublisher() {
        addBeanClass(BeanConsumingItemAsFlowableAndPublishingItemAsRSPublisher.class);
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
    public void testBeanConsumingItemAsMultiAndPublishingItemAsRSPublisher() {
        addBeanClass(BeanConsumingItemAsMultiAndPublishingItemAsRSPublisher.class);
        assertThatThrownBy(this::initialize).isInstanceOf(DeploymentException.class);
    }

    @Test
    public void testBeanConsumingItemAsFluxAndPublishingItemAsPublisher() {
        addBeanClass(BeanConsumingItemAsFluxAndPublishingItemAsPublisher.class);
        assertThatThrownBy(this::initialize).isInstanceOf(DeploymentException.class);
    }

    @Test
    public void testBeanConsumingItemAsFluxAndPublishingItemAsRSPublisher() {
        addBeanClass(BeanConsumingItemAsFluxAndPublishingItemAsRSPublisher.class);
        initialize();
        MyCollector collector = container.getBeanManager().createInstance().select(MyCollector.class).get();
        assertThat(collector.payloads()).isEqualTo(EXPECTED);
    }

    @Test
    public void testBeanConsumingItemAsPublisherAndPublishingItemAsFlowable() {
        addBeanClass(BeanConsumingItemAsPublisherAndPublishingItemAsFlowable.class);
        assertThatThrownBy(this::initialize).isInstanceOf(DeploymentException.class);
    }

    @Test
    public void testBeanConsumingItemAsRSPublisherAndPublishingItemAsFlowable() {
        addBeanClass(BeanConsumingItemAsRSPublisherAndPublishingItemAsFlowable.class);
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
    public void testBeanConsumingItemAsRSPublisherAndPublishingItemAsMulti() {
        addBeanClass(BeanConsumingItemAsRSPublisherAndPublishingItemAsMulti.class);
        assertThatThrownBy(this::initialize).isInstanceOf(DeploymentException.class);
    }

    @Test
    public void testBeanConsumingItemAsPublisherBuilderAndPublishingItemAsPublisherBuilder() {
        addBeanClass(BeanConsumingItemAsPublisherBuilderAndPublishingItemAsPublisherBuilder.class);
        initialize();
        MyCollector collector = container.getBeanManager().createInstance().select(MyCollector.class).get();
        assertThat(collector.payloads()).isEqualTo(EXPECTED);
    }

}
