package io.smallrye.reactive.messaging.inject;

import io.reactivex.Flowable;
import io.smallrye.reactive.messaging.annotations.Stream;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.List;

@ApplicationScoped
public class BeanInjectedWithAFlowableOfPayloads {

  private final Flowable<String> constructor;
  @Inject
  @Stream("hello")
  private Flowable<String> field;

  @Inject
  public BeanInjectedWithAFlowableOfPayloads(@Stream("bonjour") Flowable<String> constructor) {
    this.constructor = constructor;
  }

  public List<String> consume() {
    return Flowable
      .concat(
        Flowable.fromPublisher(constructor),
        Flowable.fromPublisher(field)
      )
      .toList()
      .blockingGet();
  }

}
