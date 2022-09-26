package io.smallrye.reactive.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import jakarta.enterprise.inject.spi.DeploymentException;

import org.junit.jupiter.api.Test;

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
        assertThatThrownBy(this::initialize).isInstanceOf(DeploymentException.class);
    }

    @Test
    public void testBeanConsumingMsgAsFlowableAndPublishingMsgAsPublisherBuilder() {
        addBeanClass(BeanConsumingMsgAsFlowableAndPublishingMsgAsPublisherBuilder.class);
        assertThatThrownBy(this::initialize).isInstanceOf(DeploymentException.class);
    }

    @Test
    public void testBeanConsumingMsgAsFlowableAndPublishingMsgAsRSPublisher() {
        addBeanClass(BeanConsumingMsgAsFlowableAndPublishingMsgAsRSPublisher.class);
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
        assertThatThrownBy(this::initialize).isInstanceOf(DeploymentException.class);
    }

    @Test
    public void testBeanConsumingMsgAsFluxAndPublishingMsgAsRSPublisher() {
        addBeanClass(BeanConsumingMsgAsFluxAndPublishingMsgAsRSPublisher.class);
        initialize();
        MyCollector collector = container.select(MyCollector.class).get();
        assertThat(collector.payloads()).isEqualTo(EXPECTED);
    }

    @Test
    public void testBeanConsumingMsgAsPublisherAndPublishingMsgAsFlowable() {
        addBeanClass(BeanConsumingMsgAsPublisherAndPublishingMsgAsFlowable.class);
        assertThatThrownBy(this::initialize).isInstanceOf(DeploymentException.class);
    }

    @Test
    public void testBeanConsumingMsgAsPublisherBuilderAndPublishingMsgAsFlowable() {
        addBeanClass(BeanConsumingMsgAsPublisherBuilderAndPublishingMsgAsFlowable.class);
        assertThatThrownBy(this::initialize).isInstanceOf(DeploymentException.class);
    }

    @Test
    public void testBeanConsumingMsgAsRSPublisherAndPublishingMsgAsFlowable() {
        addBeanClass(BeanConsumingMsgAsRSPublisherAndPublishingMsgAsFlowable.class);
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
    public void testBeanConsumingMsgAsRSPublisherAndPublishingMsgAsMulti() {
        addBeanClass(BeanConsumingMsgAsRSPublisherAndPublishingMsgAsMulti.class);
        assertThatThrownBy(this::initialize).isInstanceOf(DeploymentException.class);
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
    public void testBeanProducingARSProcessor() {
        addBeanClass(BeanProducingARSProcessorOfMessages.class);
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
