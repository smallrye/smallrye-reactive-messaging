package io.smallrye.reactive.messaging.beans;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import javax.enterprise.context.ApplicationScoped;
import java.util.concurrent.atomic.AtomicInteger;

@ApplicationScoped
public class BeanReturningMessages {

  private AtomicInteger count = new AtomicInteger();

  @Outgoing("infinite-producer")
  public Message<Integer> create() {
    return Message.of(count.incrementAndGet());
  }

}
