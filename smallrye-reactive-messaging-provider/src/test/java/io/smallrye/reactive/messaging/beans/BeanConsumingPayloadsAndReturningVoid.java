package io.smallrye.reactive.messaging.beans;

import org.eclipse.microprofile.reactive.messaging.Incoming;

import javax.enterprise.context.ApplicationScoped;
import java.util.ArrayList;
import java.util.List;

@ApplicationScoped
public class BeanConsumingPayloadsAndReturningVoid {

  private List<String> list = new ArrayList<>();

  @Incoming("count")
  public void consume(String payload) {
    list.add(payload);
  }

  public List<String> payloads() {
    return list;
  }
}
