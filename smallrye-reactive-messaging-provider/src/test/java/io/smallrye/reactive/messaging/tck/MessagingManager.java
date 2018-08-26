package io.smallrye.reactive.messaging.tck;

import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.FlowableEmitter;
import io.reactivex.FlowableOnSubscribe;
import io.reactivex.processors.MulticastProcessor;
import io.reactivex.processors.ReplayProcessor;
import io.smallrye.reactive.messaging.StreamRegistar;
import io.smallrye.reactive.messaging.StreamRegistry;
import io.smallrye.reactive.messaging.tck.incoming.Bean;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

@ApplicationScoped
public class MessagingManager implements StreamRegistar {

  private static final Message<MockPayload> DUMB = Message.of(new MockPayload());

  private Map<String, Source> topics = new HashMap<>();

  @Inject
  private StreamRegistry registry;

  private final Map<String, MockedReceiver<?>> receivers = new ConcurrentHashMap<>();
  private final Map<String, MockedSender<?>> senders = new ConcurrentHashMap<>();

  public <T> MockedReceiver<T> getReceiver(String topic) {
    return (MockedReceiver<T>) receivers.computeIfAbsent(topic, t -> new MockedReceiver<T>(Duration.ofMillis(1000), topic));
  }

  public <T> MockedSender<T> getSender(String topic) {
    return (MockedSender<T>) senders.computeIfAbsent(topic, t -> new MockedSender<T>());
  }

  public Collection<String> listTopics() {
    return topics.keySet();
  }

  public void deleteTopics(Collection<String> toDelete) {
    toDelete.forEach(s -> {
      Source source = topics.remove(s);
      if (source != null) {
        registry.unregisterPublisher(s);
        registry.unregisterSubscriber(s);
      }
    });
  }

  public void createTopics(List<String> newTopics) {
    newTopics.forEach(name -> {
      Source source = new Source();
      topics.put(name, source);
      this.registry.register(name, source.source());
      this.registry.register(name, source.subscriber());
    });
  }

  public void send(String topic, Message<MockPayload> message) {
    Source processor = topics.get(topic);
    Objects.requireNonNull(processor);
    Objects.requireNonNull(message);
    System.out.println("sending a message to topic " + topic + " / " + message + " / " + message.getPayload());
    processor.send(message);
  }

  public Optional<Message<MockPayload>> getLast(String topic, Duration timeout) {
    System.out.println("retrieving a message from topic " + topic);
    CompletableFuture<Message<MockPayload>> future = new CompletableFuture<>();
    Source source = topics.get(topic);
    Objects.requireNonNull(source);
    source.source().firstElement().subscribe(
      future::complete,
      future::completeExceptionally,
      () -> future.complete(DUMB)
    );

    try {
      return Optional.ofNullable(future.get(timeout.toMillis(), TimeUnit.MILLISECONDS));
    } catch (TimeoutException e) {
      return Optional.empty();
    } catch (Exception e) {
      throw new CompletionException(e.getCause());
    }
  }

  public void sendPayloads(String topic, MockPayload... payloads) {
    Source source = topics.get(topic);
    for (MockPayload payload : payloads) {
      source.sendWithAcknowledgement(payload);
    }
  }

  @Override
  public CompletionStage<Void> initialize() {
    createTopics(Arrays.asList(
      Bean.VOID_METHOD,
      Bean.NON_PARALLEL,
      Bean.ASYNC_FAILING,
      Bean.INCOMING_OUTGOING_WRAPPED,
      Bean.NON_VOID_METHOD,
      Bean.OUTGOING_WRAPPED,
      Bean.SYNC_FAILING,
      Bean.WRAPPED_MESSAGE
    ));
    return CompletableFuture.completedFuture(null);
  }

  private class Source  {

    private List<Message<MockPayload>> acknowledged = new ArrayList<>();
    private ReplayProcessor<Message<MockPayload>> processor = ReplayProcessor.create();
    private Flowable<Message<MockPayload>> source = processor.filter(msg -> {
      System.out.println("filtering? " + ! acknowledged.contains(msg) + " " + msg);
      return ! acknowledged.contains(msg);
    });

    public void sendWithAcknowledgement(MockPayload payload) {
      System.out.println("Sending with ACK " + payload);
      AtomicReference<Message<MockPayload>> reference = new AtomicReference<>();
      Message<MockPayload> msg = Message.of(payload, () -> {
        acknowledged.add(reference.get());
        return CompletableFuture.completedFuture(null);
      });
      reference.set(msg);
      send(msg);
    }

    public void send(Message<MockPayload> msg) {
      processor.onNext(msg);
    }

    public Flowable<Message<MockPayload>> source() {
      return source;
    }

    public Subscriber<Message<MockPayload>> subscriber() {
      return processor;
    }

  }
}
