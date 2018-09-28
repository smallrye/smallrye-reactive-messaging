package io.smallrye.reactive.messaging.providers;

import javax.enterprise.inject.se.SeContainer;

import io.smallrye.reactive.messaging.WeldTestBase;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ConfiguredSourceAndSinkTest extends WeldTestBase {

  @Test
  public void test() {
    initializer.addBeanClasses(DummyBean.class, io.smallrye.config.inject.ConfigProducer.class);

    initialize();

    assertThat(registry(container).getPublisher("dummy-source")).isNotEmpty();
    assertThat(registry(container).getPublisher("dummy-sink")).isNotEmpty();

    MyDummyFactories bean = container.select(MyDummyFactories.class).get();
    assertThat(bean.list()).containsExactly("8", "10", "12");
    assertThat(bean.gotCompletion()).isTrue();
  }

}
