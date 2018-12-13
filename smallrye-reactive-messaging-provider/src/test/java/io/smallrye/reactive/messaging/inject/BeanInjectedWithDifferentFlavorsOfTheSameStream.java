package io.smallrye.reactive.messaging.inject;

import io.reactivex.Flowable;
import io.smallrye.reactive.messaging.annotations.Stream;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.reactivestreams.Publisher;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.inject.Named;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class BeanInjectedWithDifferentFlavorsOfTheSameStream {

  @Inject
  @Stream("hello")
  private Flowable<Message<String>> field1;

  @Inject
  @Stream("hello")
  private Flowable<Message<String>> field2;

  @Inject
  @Stream("hello")
  private Publisher<Message<String>> field3;

  @Inject
  @Stream("hello")
  private Publisher<Message> field4;

  @Inject
  @Stream("hello")
  private Flowable<Message> field5;

  @Inject
  @Stream("hello")
  private PublisherBuilder<Message> field6;

  @Inject
  @Stream("hello")
  private PublisherBuilder<Message<String>> field7;

  @Inject
  @Stream("hello")
  private PublisherBuilder<String> field8;

  @Inject
  @Stream("hello")
  private Publisher<String> field9;

  @Inject
  @Stream("hello")
  private Flowable<String> field10;

  public Map<String, String> consume() {
    Map<String, String> map = new LinkedHashMap<>();
    map.put("1", field1.toString());
    map.put("2", field2.toString());
    map.put("3", field3.toString());
    map.put("4", field4.toString());
    map.put("5", field5.toString());
    map.put("6", field6.toString());
    map.put("7", field7.toString());
    map.put("8", field8.toString());
    map.put("9", field9.toString());
    map.put("10", field10.toString());
    return map;
  }

}
