package io.smallrye.reactive.messaging.inject;

import io.reactivex.Flowable;
import io.smallrye.reactive.messaging.annotations.Stream;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.List;

@ApplicationScoped
public class BeanInjectedWithAPublisherBuilderOfMessages {

  private final PublisherBuilder<Message<String>> constructor;
  @Inject
  @Stream("hello")
  private PublisherBuilder<Message<String>> field;

  @Inject
  public BeanInjectedWithAPublisherBuilderOfMessages(@Stream("bonjour") PublisherBuilder<Message<String>> constructor) {
    this.constructor = constructor;
  }

  public List<String> consume() {
    return Flowable.fromPublisher(
      ReactiveStreams.concat(constructor, field).map(Message::getPayload).buildRs()
    )
      .toList()
      .blockingGet();
  }

}
