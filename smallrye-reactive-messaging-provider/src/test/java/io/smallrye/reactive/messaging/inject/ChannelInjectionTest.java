package io.smallrye.reactive.messaging.inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

import javax.enterprise.inject.spi.DeploymentException;

import org.junit.Test;

import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;

public class ChannelInjectionTest extends WeldTestBaseWithoutTails {

    @Test
    public void testInjectionOfPublisherOfMessages() {
        addBeanClass(SourceBean.class);
        BeanInjectedWithAPublisherOfMessages bean = installInitializeAndGet(BeanInjectedWithAPublisherOfMessages.class);
        assertThat(bean.consume())
                .containsExactlyInAnyOrder("B", "O", "N", "J", "O", "U", "R", "h", "e", "l", "l", "o");
    }

    @Test
    public void testInjectionOfMultiOfMessages() {
        addBeanClass(SourceBean.class);
        BeanInjectedWithAMultiOfMessages bean = installInitializeAndGet(BeanInjectedWithAMultiOfMessages.class);
        assertThat(bean.consume())
                .containsExactlyInAnyOrder("B", "O", "N", "J", "O", "U", "R", "h", "e", "l", "l", "o");
    }

    @Test
    public void testInjectionOfFlowableOfMessages() {
        addBeanClass(SourceBean.class);
        BeanInjectedWithAFlowableOfMessages bean = installInitializeAndGet(BeanInjectedWithAFlowableOfMessages.class);
        assertThat(bean.consume())
                .containsExactlyInAnyOrder("B", "O", "N", "J", "O", "U", "R", "h", "e", "l", "l", "o");
    }

    @Test
    public void testInjectionOfMultiOfPayloads() {
        addBeanClass(SourceBean.class);
        BeanInjectedWithAMultiOfPayloads bean = installInitializeAndGet(BeanInjectedWithAMultiOfPayloads.class);
        assertThat(bean.consume())
                .containsExactlyInAnyOrder("B", "O", "N", "J", "O", "U", "R", "h", "e", "l", "l", "o");
    }

    @Test
    public void testInjectionOfFlowableOfPayloads() {
        addBeanClass(SourceBean.class);
        BeanInjectedWithAFlowableOfPayloads bean = installInitializeAndGet(BeanInjectedWithAFlowableOfPayloads.class);
        assertThat(bean.consume())
                .containsExactlyInAnyOrder("B", "O", "N", "J", "O", "U", "R", "h", "e", "l", "l", "o");
    }

    @Test
    public void testInjectionOfPublisherOfPayloads() {
        addBeanClass(SourceBean.class);
        BeanInjectedWithAPublisherOfPayloads bean = installInitializeAndGet(BeanInjectedWithAPublisherOfPayloads.class);
        assertThat(bean.consume())
                .containsExactlyInAnyOrder("B", "O", "N", "J", "O", "U", "R", "h", "e", "l", "l", "o");
    }

    @Test
    public void testInjectionOfPublisherBuilderOfPayloads() {
        addBeanClass(SourceBean.class);
        BeanInjectedWithAPublisherBuilderOfPayloads bean = installInitializeAndGet(
                BeanInjectedWithAPublisherBuilderOfPayloads.class);
        assertThat(bean.consume())
                .containsExactlyInAnyOrder("B", "O", "N", "J", "O", "U", "R", "h", "e", "l", "l", "o");
    }

    @Test
    public void testInjectionOfPublisherBuilderOfMessages() {
        addBeanClass(SourceBean.class);
        BeanInjectedWithAPublisherBuilderOfMessages bean = installInitializeAndGet(
                BeanInjectedWithAPublisherBuilderOfMessages.class);
        assertThat(bean.consume())
                .containsExactlyInAnyOrder("B", "O", "N", "J", "O", "U", "R", "h", "e", "l", "l", "o");
    }

    @Test
    public void testMultipleFieldInjection() {
        addBeanClass(SourceBean.class);
        BeanInjectedWithDifferentFlavorsOfTheSameChannel bean = installInitializeAndGet(
                BeanInjectedWithDifferentFlavorsOfTheSameChannel.class);
        assertThat(bean.consume()).hasSize(14);
    }

    @Test
    public void testNonExistentChannel() {
        addBeanClass(SourceBean.class);
        try {
            installInitializeAndGet(BeanInjectedNonExistentChannel.class);
            fail();
        } catch (DeploymentException expected) {
        }
    }

    @Test
    public void testMultipleFieldInjectionLegacyChannel() {
        addBeanClass(SourceBean.class);
        BeanInjectedWithDifferentFlavorsOfTheSameChannelLegacy bean = installInitializeAndGet(
                BeanInjectedWithDifferentFlavorsOfTheSameChannelLegacy.class);
        assertThat(bean.consume()).hasSize(10);
    }

    @Test
    public void testNonExistentLEgacyChannel() {
        addBeanClass(SourceBean.class);
        try {
            installInitializeAndGet(BeanInjectedNonExistentLegacyChannel.class);
            fail();
        } catch (DeploymentException expected) {
        }
    }

}
