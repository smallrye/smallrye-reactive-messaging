package io.smallrye.reactive.messaging.inject;

import io.reactivex.Flowable;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.reactivestreams.Publisher;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class SourceBean {

  @Outgoing("hello")
  public Publisher<String> hello() {
    return Flowable.fromArray("h", "e", "l", "l", "o");
  }

  @Outgoing("bonjour")
  @Incoming("raw")
  public Flowable<String> bonjour(Flowable<String> input) {
    return input.map(String::toUpperCase);
  }

  @Outgoing("raw")
  public Flowable<String> raw() {
    return Flowable.fromArray("b", "o", "n", "j", "o", "u", "r");
  }

}
