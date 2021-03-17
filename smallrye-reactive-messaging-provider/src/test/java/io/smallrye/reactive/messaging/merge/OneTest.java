package io.smallrye.reactive.messaging.merge;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;

public class OneTest extends WeldTestBaseWithoutTails {

    @Override
    public List<Class<?>> getBeans() {
        return Collections.singletonList(BeanUsingOne.class);
    }

    @Test
    public void testOne() {
        initialize();
        BeanUsingOne merge = container.getBeanManager().createInstance().select(BeanUsingOne.class).get();
        await().until(() -> merge.list().size() == 3);
        if (merge.list().get(0).equalsIgnoreCase("a")) {
            assertThat(merge.list()).containsExactly("a", "b", "c");
        } else {
            assertThat(merge.list()).containsExactly("D", "E", "F");
        }
    }

}
