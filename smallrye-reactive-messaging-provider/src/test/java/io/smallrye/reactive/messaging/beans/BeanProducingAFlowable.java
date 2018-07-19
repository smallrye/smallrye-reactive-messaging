package io.smallrye.reactive.messaging.beans;

import io.reactivex.Flowable;
import io.smallrye.reactive.messaging.DefaultMessage;
import org.eclipse.microprofile.reactive.messaging.Message;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Named;

@ApplicationScoped
public class BeanProducingAFlowable {

  @Produces
  @Named("producer")
  public Flowable<Message<String>> create() {
    return Flowable.just("a", "b", "c").map(DefaultMessage::create);
  }

}
