package io.smallrye.reactive.messaging.tck;

import io.reactivex.Flowable;
import io.reactivex.processors.PublishProcessor;
import io.smallrye.reactive.messaging.StreamRegistar;
import io.smallrye.reactive.messaging.StreamRegistry;
import io.smallrye.reactive.messaging.tck.incoming.Bean;
import org.eclipse.microprofile.reactive.messaging.Message;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

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

  public void createTopics(List<String> newTopics) {
    newTopics.forEach(name -> {
      Source source = new Source(name);
      topics.put(name, source);
      this.registry.register(name, source.source());
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

  private class Source {

    public Source(String name) {
      this.name = name;
    }

    private String name;
    private List<Message<MockPayload>> inflights = new ArrayList<>();
    private PublishProcessor<Message<MockPayload>> processor = PublishProcessor.create();
    private Flowable<Message<MockPayload>> source = processor
      .doOnCancel(() -> System.out.println("Cancellation caught"))
      .doOnSubscribe((x) -> resend());

    private void resend() {
      System.out.println("Resending... " + inflights.stream().map(Message::getPayload).collect(Collectors.toList()));
      new ArrayList<>(inflights).forEach(m -> sendWithAcknowledgement(m.getPayload()));
    }

    public void sendWithAcknowledgement(MockPayload payload) {
      System.out.println("Sending with ACK " + payload);
      AtomicReference<Message<MockPayload>> reference = new AtomicReference<>();
      Message<MockPayload> msg = Message.of(payload, () -> {
        System.out.println("Acknowledging " + payload);
        inflights.remove(reference.get());
        return CompletableFuture.completedFuture(null);
      });
      reference.set(msg);
      send(msg);
    }

    public void send(Message<MockPayload> msg) {
      inflights.add(msg);
      System.out.println("Sending message " + msg + " on " + name);
      if (processor != null) {
        processor.onNext(msg);
      }
    }

    public Flowable<Message<MockPayload>> source() {
      return source;
    }

  }
}
