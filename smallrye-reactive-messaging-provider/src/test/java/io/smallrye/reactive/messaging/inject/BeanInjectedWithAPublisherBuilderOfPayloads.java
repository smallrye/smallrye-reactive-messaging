package io.smallrye.reactive.messaging.inject;

import io.reactivex.Flowable;
import io.smallrye.reactive.messaging.annotations.Stream;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.ReactiveStreams;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.List;

@ApplicationScoped
public class BeanInjectedWithAPublisherBuilderOfPayloads {

  private final PublisherBuilder<String> constructor;
  @Inject
  @Stream("hello")
  private PublisherBuilder<String> field;

  @Inject
  public BeanInjectedWithAPublisherBuilderOfPayloads(@Stream("bonjour") PublisherBuilder<String> constructor) {
    this.constructor = constructor;
  }

  public List<String> consume() {
    return Flowable.fromPublisher(
      ReactiveStreams.concat(constructor, field).buildRs()
    )
      .toList()
      .blockingGet();
  }

}
