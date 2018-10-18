package io.smallrye.reactive.messaging.multicast;

import io.reactivex.Flowable;
import io.smallrye.reactive.messaging.annotations.Multicast;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.ReactiveStreams;
import org.reactivestreams.Publisher;

import javax.enterprise.context.ApplicationScoped;
import java.util.ArrayList;
import java.util.List;

@ApplicationScoped
public class BeanUsingMulticast {

  private List<String> l1 = new ArrayList<>();
  private List<String> l2 = new ArrayList<>();

  @Incoming("Y")
  public void y2(String i) {
    l2.add(i);
  }

  @Outgoing("X")
  public Publisher<String> x() {
    return ReactiveStreams.of("a", "b", "c", "d").buildRs();
  }

  @Outgoing("Y")
  @Incoming("X")
  @Multicast(2)
  public String process(String s) {
    return s.toUpperCase();
  }

  @Incoming("Y")
  public void y1(String i) {
    l1.add(i);
  }

  public List<String> l1() {
    return l1;
  }

  public List<String> l2() {
    return l2;
  }


}
