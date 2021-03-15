package io.smallrye.reactive.messaging.merge;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;

public class ConcatTest extends WeldTestBaseWithoutTails {

    @Override
    public List<Class<?>> getBeans() {
        return Collections.singletonList(BeanUsingConcat.class);
    }

    @Test
    public void testConcat() {
        initialize();
        BeanUsingConcat merge = container.getBeanManager().createInstance().select(BeanUsingConcat.class).get();
        await().until(() -> merge.list().size() == 7);
        assertThat(merge.list()).contains("a", "b", "c", "D", "E", "F", "G");
        if (merge.list().get(0).equalsIgnoreCase("a")) {
            assertThat(merge.list()).containsExactly("a", "b", "c", "D", "E", "F", "G");
        } else {
            assertThat(merge.list()).containsExactly("D", "E", "F", "G", "a", "b", "c");
        }
    }

}
