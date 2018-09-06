package io.smallrye.reactive.messaging.beans;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

import javax.enterprise.context.ApplicationScoped;
import java.util.ArrayList;
import java.util.List;

@ApplicationScoped
public class BeanConsumingMessagesAndReturningSomething {

  private List<String> list = new ArrayList<>();


  @Incoming("count")
  public String consume(Message<String> message) {
    list.add(message.getPayload());
    return message.getPayload();
  }

  public List<String> payloads() {
    return list;
  }
}
