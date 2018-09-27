package io.smallrye.reactive.messaging;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ReactiveMessagingExtensionTest extends WeldTestBase {


  @Test
  public void test() {
    initializer.addBeanClasses(MyBean.class);
    initializer.initialize();
    assertThat(MyBean.COLLECTOR).containsExactly("FOO", "FOO", "BAR", "BAR");
  }

}
