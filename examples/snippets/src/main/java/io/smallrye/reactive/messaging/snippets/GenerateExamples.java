package io.smallrye.reactive.messaging.snippets;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;

import javax.enterprise.context.ApplicationScoped;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

@ApplicationScoped
public class GenerateExamples {

  /**
   * Produces a simple stream. The produced payloads are automatically wrapped
   * into Messages.
   */
  @Outgoing("data-1")
  public PublisherBuilder<String> generate() {
    return ReactiveStreams.of("a", "b", "c");
  }

  /**
   * Produces a simple stream of Message.
   */
  @Outgoing("data-2")
  public PublisherBuilder<Message<String>> generateMessages() {
    return ReactiveStreams.of("a", "b", "c").map(Message::of);
  }

  private AtomicInteger counter = new AtomicInteger();

  /**
   * Generates a stream payload by payload.
   */
  @Outgoing("data-3")
  public int count() {
    return counter.incrementAndGet();
  }

  /**
   * Generates a stream message by message.
   */
  @Outgoing("data-4")
  public Message<Integer> countAsMessage() {
    return Message.of(counter.incrementAndGet());
  }

  private Executor executor = Executors.newSingleThreadExecutor();

  /**
   * Generates a stream message by message. The next message is only generated
   * when the value from the previous one is redeemed.
   */
  @Outgoing("data-5")
  public CompletionStage<Message<Integer>> produceAsyncMessages() {
    return CompletableFuture.supplyAsync(() -> Message.of(counter.incrementAndGet()), executor);
  }

}
