package io.smallrye.reactive.messaging.merge;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;

public class MergeTest extends WeldTestBaseWithoutTails {

    @Override
    public List<Class<?>> getBeans() {
        return Collections.singletonList(BeanUsingMerge.class);
    }

    @Test
    public void testRegularMerge() {
        initialize();
        BeanUsingMerge merge = container.getBeanManager().createInstance().select(BeanUsingMerge.class).get();
        await().until(() -> merge.list().size() == 7);
        assertThat(merge.list()).contains("a", "b", "c", "D", "E", "F", "G");
    }

}
