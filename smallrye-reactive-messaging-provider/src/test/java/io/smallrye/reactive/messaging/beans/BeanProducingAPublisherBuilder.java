package io.smallrye.reactive.messaging.beans;

import io.smallrye.reactive.messaging.DefaultMessage;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.ReactiveStreams;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Named;

@ApplicationScoped
public class BeanProducingAPublisherBuilder {

  @Produces
  @Named("producer")
  public PublisherBuilder<Message<String>> create() {
    return ReactiveStreams.of("a", "b", "c").map(DefaultMessage::create);
  }

}
