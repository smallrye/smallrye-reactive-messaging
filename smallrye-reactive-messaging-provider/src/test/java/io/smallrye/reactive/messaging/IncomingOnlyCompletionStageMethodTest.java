package io.smallrye.reactive.messaging;

import io.smallrye.reactive.messaging.beans.IncomingOnlyBeanProducingACompletableStage;
import io.smallrye.reactive.messaging.beans.IncomingOnlyBeanProducingANonVoidCompletableStage;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class IncomingOnlyCompletionStageMethodTest extends WeldTestBaseWithoutTails {


  @Override
  public List<Class> getBeans() {
    return Collections.singletonList(SourceOnly.class);
  }

  @Test
  public void testIncomingOnlyBeanProducingACompletionStageOfVoid() {
    weld.addBeanClass(IncomingOnlyBeanProducingACompletableStage.class);
    WeldContainer container = weld.initialize();
    IncomingOnlyBeanProducingACompletableStage collector = container.getBeanManager().createInstance()
      .select(IncomingOnlyBeanProducingACompletableStage.class).get();
    assertThat(collector.list()).isNotEmpty()
    .containsExactly(1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 10);
  }

  @Test
  public void testIncomingOnlyBeanProducingACompletionStageNonVoid() {
    weld.addBeanClass(IncomingOnlyBeanProducingANonVoidCompletableStage.class);
    WeldContainer container = weld.initialize();
    IncomingOnlyBeanProducingANonVoidCompletableStage collector = container.getBeanManager().createInstance()
      .select(IncomingOnlyBeanProducingANonVoidCompletableStage.class).get();
    assertThat(collector.list()).isNotEmpty()
      .containsExactly(1, 1, 2, 2,  3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 10);
  }


}
