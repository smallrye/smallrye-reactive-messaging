package io.smallrye.reactive.messaging.providers;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.operators.ProcessorBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class DummyBean {

  @Incoming(value = "dummy-source", provider = Dummy.class)
  @Outgoing(value = "dummy-sink", provider = Dummy.class)
  public ProcessorBuilder<Integer, String> process() {
    return ReactiveStreams.<Integer>builder().map(i -> i * 2)
      .map(i -> Integer.toString(i));
  }

}
