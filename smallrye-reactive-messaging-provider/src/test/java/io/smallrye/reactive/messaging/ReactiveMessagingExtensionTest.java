package io.smallrye.reactive.messaging;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class ReactiveMessagingExtensionTest extends WeldTestBase {

    @Test
    public void test() {
        addBeanClass(MyBean.class);
        initialize();
        assertThat(MyBean.COLLECTOR).containsExactly("FOO", "FOO", "BAR", "BAR");
    }

}
