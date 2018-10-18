package io.smallrye.reactive.messaging.camel.incoming;

import io.reactivex.Flowable;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.reactive.streams.api.CamelReactiveStreamsService;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.reactivestreams.Subscriber;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@ApplicationScoped
public class BeanWithCamelSubscriberFromReactiveStreamRoute extends RouteBuilder {

  @Inject
  private CamelReactiveStreamsService reactive;

  @Incoming("camel")
  public Subscriber<String> sink() {
    return reactive.streamSubscriber("camel-sub", String.class);
  }

  @Outgoing("camel")
  public Flowable<String> source() {
    return Flowable.fromArray("a", "b", "c", "d");
  }

  @Override
  public void configure() {
    from("reactive-streams:camel-sub")
      .to("file:./target?fileName=values.txt&fileExist=append");
  }
}
