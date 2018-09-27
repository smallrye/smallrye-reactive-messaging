package io.smallrye.reactive.messaging;

import java.util.Collections;
import java.util.List;

import javax.enterprise.inject.se.SeContainer;

import io.smallrye.reactive.messaging.beans.IncomingOnlyBeanProducingACompletableStage;
import io.smallrye.reactive.messaging.beans.IncomingOnlyBeanProducingANonVoidCompletableStage;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class IncomingOnlyCompletionStageMethodTest extends WeldTestBaseWithoutTails {


  @Override
  public List<Class> getBeans() {
    return Collections.singletonList(SourceOnly.class);
  }

  @Test
  public void testIncomingOnlyBeanProducingACompletionStageOfVoid() {
    initializer.addBeanClasses(IncomingOnlyBeanProducingACompletableStage.class);
    SeContainer container = initializer.initialize();
    IncomingOnlyBeanProducingACompletableStage collector = container
      .select(IncomingOnlyBeanProducingACompletableStage.class).get();
    assertThat(collector.list()).isNotEmpty()
    .containsExactly(1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 10);
  }

  @Test
  public void testIncomingOnlyBeanProducingACompletionStageNonVoid() {
    initializer.addBeanClasses(IncomingOnlyBeanProducingANonVoidCompletableStage.class);
    SeContainer container = initializer.initialize();
    IncomingOnlyBeanProducingANonVoidCompletableStage collector = container
      .select(IncomingOnlyBeanProducingANonVoidCompletableStage.class).get();
    assertThat(collector.list()).isNotEmpty()
      .containsExactly(1, 1, 2, 2,  3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 10);
  }


}
