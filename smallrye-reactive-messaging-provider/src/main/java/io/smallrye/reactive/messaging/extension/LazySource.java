package io.smallrye.reactive.messaging.extension;

import io.reactivex.Flowable;
import io.smallrye.reactive.messaging.StreamRegistry;
import io.smallrye.reactive.messaging.annotations.Merge;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.slf4j.Logger;

import java.util.List;

class LazySource implements Publisher<Message> {
  private Publisher<? extends Message> delegate;
  private String source;
  private Merge.Mode mode;

  LazySource(String source, Merge.Mode mode) {
    this.source = source;
    this.mode = mode;
  }

  public void configure(StreamRegistry registry, Logger logger) {
    List<Publisher<? extends Message>> list = registry.getPublishers(source);
    if (! list.isEmpty()) {
      switch (mode) {
        case MERGE: this.delegate = Flowable.merge(list); break;
        case ONE:
          this.delegate = list.get(0);
          if (list.size() > 1) {
            logger.warn("Multiple publisher found for {}, using the merge policy `ONE` takes the first found", source);
          }
          break;

        case CONCAT: this.delegate = Flowable.concat(list); break;
        default: throw new IllegalArgumentException("Unknown merge policy for " + source +  ": " + mode);
      }
    }
  }

  @Override
  public void subscribe(Subscriber<? super Message> s) {
    delegate.subscribe(s);
  }
}
