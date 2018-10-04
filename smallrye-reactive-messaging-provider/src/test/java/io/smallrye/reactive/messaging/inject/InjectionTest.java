package io.smallrye.reactive.messaging.inject;

import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class InjectionTest extends WeldTestBaseWithoutTails {


  @Test
  public void testInjectionOfPublisherOfMessages() {
    addBeanClass(SourceBean.class);
    BeanInjectedWithAPublisherOfMessages bean  = installInitializeAndGet(BeanInjectedWithAPublisherOfMessages.class);
    assertThat(bean.consume()).containsExactlyInAnyOrder("B", "O", "N", "J", "O", "U", "R", "h", "e", "l", "l", "o");
  }

  @Test
  public void testInjectionOfFlowableOfMessages() {
    addBeanClass(SourceBean.class);
    BeanInjectedWithAFlowableOfMessages bean  = installInitializeAndGet(BeanInjectedWithAFlowableOfMessages.class);
    assertThat(bean.consume()).containsExactlyInAnyOrder("B", "O", "N", "J", "O", "U", "R", "h", "e", "l", "l", "o");
  }

  @Test
  public void testInjectionOfFlowableOfPayloads() {
    addBeanClass(SourceBean.class);
    BeanInjectedWithAFlowableOfPayloads bean  = installInitializeAndGet(BeanInjectedWithAFlowableOfPayloads.class);
    assertThat(bean.consume()).containsExactlyInAnyOrder("B", "O", "N", "J", "O", "U", "R", "h", "e", "l", "l", "o");
  }

  @Test
  public void testInjectionOfPublisherOfPayloads() {
    addBeanClass(SourceBean.class);
    BeanInjectedWithAPublisherOfPayloads bean  = installInitializeAndGet(BeanInjectedWithAPublisherOfPayloads.class);
    assertThat(bean.consume()).containsExactlyInAnyOrder("B", "O", "N", "J", "O", "U", "R", "h", "e", "l", "l", "o");
  }

  @Test
  public void testInjectionOfPublisherBuilderOfPayloads() {
    addBeanClass(SourceBean.class);
    BeanInjectedWithAPublisherBuilderOfPayloads bean  = installInitializeAndGet(BeanInjectedWithAPublisherBuilderOfPayloads.class);
    assertThat(bean.consume()).containsExactlyInAnyOrder("B", "O", "N", "J", "O", "U", "R", "h", "e", "l", "l", "o");
  }

  @Test
  public void testInjectionOfPublisherBuilderOfMessages() {
    addBeanClass(SourceBean.class);
    BeanInjectedWithAPublisherBuilderOfMessages bean  = installInitializeAndGet(BeanInjectedWithAPublisherBuilderOfMessages.class);
    assertThat(bean.consume()).containsExactlyInAnyOrder("B", "O", "N", "J", "O", "U", "R", "h", "e", "l", "l", "o");
  }



  @Test
  public void testMultipleFieldInjection() {
    addBeanClass(SourceBean.class);
    BeanInjectedWithDifferentFlavorsOfTheSameStream bean = installInitializeAndGet(BeanInjectedWithDifferentFlavorsOfTheSameStream.class);
    assertThat(bean.consume()).hasSize(10);
  }

}
