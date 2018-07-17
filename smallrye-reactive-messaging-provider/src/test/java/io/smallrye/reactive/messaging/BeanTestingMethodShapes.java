package io.smallrye.reactive.messaging;

import io.reactivex.Flowable;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.ProcessorBuilder;
import org.eclipse.microprofile.reactive.streams.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.ReactiveStreams;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Named;
import java.util.ArrayList;
import java.util.List;

@ApplicationScoped
public class BeanTestingMethodShapes {

  static List<String> methodReceivingAFlowable = new ArrayList<>();
  static List<String> methodReceivingAPublisher = new ArrayList<>();
  static List<String> methodReceivingAPublisherBuilder = new ArrayList<>();
  static List<String> methodReturningAPublisherBuilder = new ArrayList<>();
  static List<String> methodProducingAProcessorBuilder = new ArrayList<>();
  static List<String> methodProducingAProcessor = new ArrayList<>();
  static List<String> methodConsumingItemsAndProducingItems = new ArrayList<>();
  static List<String> methodConsumingMessagesAndProducingItems = new ArrayList<>();
  static List<String> methodConsumingItemsAndProducingMessages = new ArrayList<>();
  static List<String> methodConsumingMessagesAndProducingMessages = new ArrayList<>();

  @Produces
  @Named("numbers")
  public Flowable<Integer> numbers() {
    return Flowable.fromArray(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
  }

  @Produces
  @Named("numbers-as-messages")
  public Flowable<Message<Integer>> messages() {
    return Flowable.fromArray(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).map(DefaultMessage::create);
  }

  @Produces
  @Named("methodReceivingAFlowable")
  public Subscriber<String> methodReceivingAFlowable() {
    return ReactiveStreams.<String>builder().peek(s -> methodReceivingAFlowable.add(s)).ignore().build();
  }

  @Produces
  @Named("methodReceivingAPublisher")
  public Subscriber<String> methodReceivingAPublisher() {
    return ReactiveStreams.<String>builder().peek(s -> methodReceivingAPublisher.add(s)).ignore().build();
  }

  @Produces
  @Named("methodReceivingAPublisherBuilder")
  public Subscriber<String> methodReceivingAPublisherBuilder() {
    return ReactiveStreams.<String>builder().peek(s -> methodReceivingAPublisherBuilder.add(s)).ignore().build();
  }

  @Produces
  @Named("methodReturningAPublisherBuilder")
  public Subscriber<String> methodReturningAPublisherBuilder() {
    return ReactiveStreams.<String>builder().peek(s -> methodReturningAPublisherBuilder.add(s)).ignore().build();
  }

  @Produces
  @Named("methodProducingAProcessorBuilder")
  public Subscriber<String> methodProducingAProcessorBuilder() {
    return ReactiveStreams.<String>builder().peek(s -> methodProducingAProcessorBuilder.add(s)).ignore().build();
  }

  @Produces
  @Named("methodConsumingItemsAndProducingItems")
  public Subscriber<Message<String>> methodConsumingItemsAndProducingItems() {
    return ReactiveStreams.<Message<String>>builder()
      .map(Message::getPayload)
      .peek(s -> methodConsumingItemsAndProducingItems.add(s)).ignore().build();
  }

  @Produces
  @Named("methodConsumingMessagesAndProducingItems")
  public Subscriber<Message<String>> methodConsumingMessagesAndProducingItems() {
    return ReactiveStreams.<Message<String>>builder()
      .map(Message::getPayload)
      .peek(s -> methodConsumingMessagesAndProducingItems.add(s)).ignore().build();
  }

  @Produces
  @Named("methodConsumingMessagesAndProducingMessages")
  public Subscriber<Message<String>> methodConsumingMessagesAndProducingMessages() {
    return ReactiveStreams.<Message<String>>builder()
      .map(Message::getPayload)
      .peek(s -> methodConsumingMessagesAndProducingMessages.add(s)).ignore().build();
  }

  @Produces
  @Named("methodConsumingItemsAndProducingMessages")
  public Subscriber<Message<String>> methodConsumingItemsAndProducingMessages() {
    return ReactiveStreams.<Message<String>>builder()
      .map(Message::getPayload)
      .peek(s -> methodConsumingItemsAndProducingMessages.add(s)).ignore().build();
  }

  @Produces
  @Named("methodProducingAProcessor")
  public Subscriber<String> methodProducingAProcessor() {
    return ReactiveStreams.<String>builder().peek(s -> methodProducingAProcessor.add(s)).ignore().build();
  }

  @Incoming(topic = "numbers")
  @Outgoing(topic = "methodReceivingAFlowable")
  public Flowable<String> mediatorReceivingAStream(Flowable<Integer> stream) {
    return stream
      .map(i -> i + 1)
      .map(Object::toString);
  }

  @Incoming(topic = "numbers")
  @Outgoing(topic = "methodReceivingAPublisher")
  public Publisher<String>mediatorReceivingAStream(Publisher<Integer> stream) {
    return ReactiveStreams.fromPublisher(stream)
      .map(i -> i + 1)
      .map(Object::toString)
      .buildRs();
  }

  @Incoming(topic = "numbers")
  @Outgoing(topic =  "methodReceivingAPublisherBuilder")
  public Publisher<String> mediatorReceivingAStream(PublisherBuilder<Integer> stream) {
    return stream
      .map(i -> i + 1)
      .map(Object::toString)
      .buildRs();
  }

  @Incoming(topic = "numbers")
  @Outgoing(topic = "methodReturningAPublisherBuilder")
  public PublisherBuilder<String> mediatorReceivingAPublisher(Publisher<Integer> stream) {
    return ReactiveStreams.fromPublisher(stream)
      .map(i -> i + 1)
      .map(Object::toString);
  }

  @Incoming(topic = "numbers")
  @Outgoing(topic = "methodProducingAProcessor")
  public Processor<Integer, String> mediatorProducingAProcessor() {
    return mediatorProducingAProcessorBuilder().buildRs();
  }


  @Incoming(topic = "numbers")
  @Outgoing(topic = "methodProducingAProcessorBuilder")
  public ProcessorBuilder<Integer, String> mediatorProducingAProcessorBuilder() {
    return ReactiveStreams.<Integer>builder()
      .map(i -> i + 1)
      .map(Object::toString);
  }

  @Incoming(topic = "numbers-as-messages")
  @Outgoing(topic = "methodConsumingItemsAndProducingItems")
  public String mediatorConsumingItemsAndProducingItems(int item) {
    return Integer.toString(item + 1);
  }

  @Incoming(topic = "numbers-as-messages")
  @Outgoing(topic = "methodConsumingMessagesAndProducingMessages")
  public Message<String> mediatorConsumingMessagesAndProducingMessages(Message<Integer> item) {
    return DefaultMessage.create(Integer.toString(item.getPayload() + 1));
  }

  @Incoming(topic = "numbers-as-messages")
  @Outgoing(topic = "methodConsumingItemsAndProducingMessages")
  public Message<String> mediatorConsumingItemsAndProducingItem(int item) {
    return DefaultMessage.create(Integer.toString(item + 1));
  }

  @Incoming(topic = "numbers-as-messages")
  @Outgoing(topic = "methodConsumingMessagesAndProducingItems")
  public String mediatorConsumingMessagesAndProducingItems(Message<Integer> item) {
    System.out.println("Got called...");
    return Integer.toString(item.getPayload() + 1);
  }


}
