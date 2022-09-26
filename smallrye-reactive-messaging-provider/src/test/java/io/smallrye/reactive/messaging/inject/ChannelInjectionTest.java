package io.smallrye.reactive.messaging.inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.Test;

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
    public void testInjectionOfRSPublisherOfMessages() {
        addBeanClass(SourceBean.class);
        BeanInjectedWithARSPublisherOfMessages bean = installInitializeAndGet(BeanInjectedWithARSPublisherOfMessages.class);
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
    public void testInjectionOfMultiOfPayloads() {
        addBeanClass(SourceBean.class);
        BeanInjectedWithAMultiOfPayloads bean = installInitializeAndGet(BeanInjectedWithAMultiOfPayloads.class);
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
    public void testInjectionOfRSPublisherOfPayloads() {
        addBeanClass(SourceBean.class);
        BeanInjectedWithARSPublisherOfPayloads bean = installInitializeAndGet(BeanInjectedWithARSPublisherOfPayloads.class);
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
        assertThat(bean.consume()).hasSize(10);
    }

    @Test
    public void testNonExistentChannel() {
        addBeanClass(SourceBean.class);
        BeanInjectedNonExistentChannel bean = installInitializeAndGet(BeanInjectedNonExistentChannel.class, false);
        try {
            bean.getChannel().collect().first().await().indefinitely();
        } catch (IllegalStateException e) {
            assertThat(e).hasMessageContaining("SRMSG00018");
        }
    }

    @Test
    public void testMultipleFieldInjectionLegacyChannel() {
        addBeanClass(SourceBean.class);
        BeanInjectedWithDifferentFlavorsOfTheSameChannelLegacy bean = installInitializeAndGet(
                BeanInjectedWithDifferentFlavorsOfTheSameChannelLegacy.class);
        assertThat(bean.consume()).hasSize(5);
    }

    @Test
    public void testNonExistentLegacyChannel() {
        addBeanClass(SourceBean.class);
        try {
            BeanInjectedNonExistentLegacyChannel bean = installInitializeAndGet(
                    BeanInjectedNonExistentLegacyChannel.class);
            bean.goingToFail();
            fail();
        } catch (IllegalStateException expected) {
        }
    }

}
