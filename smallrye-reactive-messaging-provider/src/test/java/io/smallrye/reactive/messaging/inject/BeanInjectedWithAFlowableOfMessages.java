package io.smallrye.reactive.messaging.inject;

import io.reactivex.Flowable;
import io.smallrye.reactive.messaging.annotations.Stream;
import org.eclipse.microprofile.reactive.messaging.Message;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.List;

@ApplicationScoped
public class BeanInjectedWithAFlowableOfMessages {

  private final Flowable<Message<String>> constructor;
  @Inject
  @Stream("hello")
  private Flowable<Message<String>> field;

  @Inject
  public BeanInjectedWithAFlowableOfMessages(@Stream("bonjour") Flowable<Message<String>> constructor) {
    this.constructor = constructor;
  }

  public List<String> consume() {
    return Flowable
      .concat(
        Flowable.fromPublisher(constructor),
        Flowable.fromPublisher(field)
      )
      .map(Message::getPayload)
      .toList()
      .blockingGet();
  }

}
