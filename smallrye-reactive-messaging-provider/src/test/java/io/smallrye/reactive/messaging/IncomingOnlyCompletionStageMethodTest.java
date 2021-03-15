package io.smallrye.reactive.messaging;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.beans.IncomingOnlyBeanProducingACompletableStage;
import io.smallrye.reactive.messaging.beans.IncomingOnlyBeanProducingANonVoidCompletableStage;

public class IncomingOnlyCompletionStageMethodTest extends WeldTestBaseWithoutTails {

    @Override
    public List<Class<?>> getBeans() {
        return Collections.singletonList(SourceOnly.class);
    }

    @Test
    public void testIncomingOnlyBeanProducingACompletionStageOfVoid() {
        addBeanClass(IncomingOnlyBeanProducingACompletableStage.class);
        initialize();
        IncomingOnlyBeanProducingACompletableStage collector = container
                .select(IncomingOnlyBeanProducingACompletableStage.class).get();
        assertThat(collector.list()).isNotEmpty()
                .containsExactly(1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 10);
    }

    @Test
    public void testIncomingOnlyBeanProducingACompletionStageNonVoid() {
        addBeanClass(IncomingOnlyBeanProducingANonVoidCompletableStage.class);
        initialize();
        IncomingOnlyBeanProducingANonVoidCompletableStage collector = container
                .select(IncomingOnlyBeanProducingANonVoidCompletableStage.class).get();
        assertThat(collector.list()).isNotEmpty()
                .containsExactly(1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 10);
    }

}
