package io.smallrye.reactive.messaging.tck.incoming;

import io.smallrye.reactive.messaging.tck.MessagingManager;
import io.smallrye.reactive.messaging.tck.MockPayload;
import io.smallrye.reactive.messaging.tck.QuietRuntimeException;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@ApplicationScoped
public class Bean {

  public static final String VOID_METHOD = "void-method";
  public static final String NON_PARALLEL = "non-parallel";
  public static final String NON_VOID_METHOD = "non-void-method";
  public static final String SYNC_FAILING = "sync-failing";
  public static final String ASYNC_FAILING = "async-failing";
  public static final String WRAPPED_MESSAGE = "wrapped-message";
  public static final String OUTGOING_WRAPPED = "outgoing-wrapped";
  public static final String INCOMING_OUTGOING_WRAPPED = "incoming-outgoing-wrapped";

  @Inject
  private MessagingManager manager;

  @Incoming(VOID_METHOD)
  public CompletionStage<Void> handleVoidMethod(MockPayload payload) {
    manager.getReceiver(VOID_METHOD).receiveMessage(payload);
    return CompletableFuture.completedFuture(null);
  }

  private final Deque<CompletableFuture<Void>> futures = new ArrayDeque<>();

  public Deque<CompletableFuture<Void>> getFutures() {
    return futures;
  }

  @Incoming(NON_PARALLEL)
  public CompletionStage<Void> handleNonParallel(MockPayload payload) {
    manager.getReceiver(NON_PARALLEL).receiveMessage(payload);
    CompletableFuture<Void> future = new CompletableFuture<>();
    futures.add(future);
    return future;
  }

  @Incoming(NON_VOID_METHOD)
  public CompletionStage<String> handleNonVoidMethod(MockPayload payload) {
    manager.getReceiver(NON_VOID_METHOD).receiveMessage(payload);
    return CompletableFuture.completedFuture("hello");
  }

  private final AtomicBoolean syncFailed = new AtomicBoolean();

  public AtomicBoolean getSyncFailed() {
    return syncFailed;
  }

  @Incoming(SYNC_FAILING)
  public CompletionStage<Void> handleSyncFailing(MockPayload payload) {
    if (payload.getField1().equals("fail") && !syncFailed.getAndSet(true)) {
      manager.getReceiver(SYNC_FAILING).receiveMessage(payload);
      throw new QuietRuntimeException("failed");
    }
    else {
      manager.getReceiver(SYNC_FAILING).receiveMessage(payload);
      return CompletableFuture.completedFuture(null);
    }
  }

  private final AtomicBoolean asyncFailed = new AtomicBoolean();

  public AtomicBoolean getAsyncFailed() {
    return asyncFailed;
  }

  @Incoming(ASYNC_FAILING)
  public CompletionStage<Void> handleAsyncFailing(MockPayload payload) {
    if (payload.getField1().equals("fail") && !asyncFailed.getAndSet(true)) {
      manager.getReceiver(ASYNC_FAILING).receiveMessage(payload);
      CompletableFuture<Void> future = new CompletableFuture<>();
      future.completeExceptionally(new QuietRuntimeException("failed"));
      return future;
    }
    else {
      manager.getReceiver(ASYNC_FAILING).receiveMessage(payload);
      return CompletableFuture.completedFuture(null);
    }
  }

  private final AtomicBoolean wrappedFailed = new AtomicBoolean();

  public AtomicBoolean getWrappedFailed() {
    return wrappedFailed;
  }

  @Incoming(WRAPPED_MESSAGE)
  public CompletionStage<Void> handleWrapped(Message<MockPayload> msg) {
    if (msg.getPayload().getField1().equals("acknowledged") || wrappedFailed.get()) {
      manager.<MockPayload>getReceiver(WRAPPED_MESSAGE).receiveWrappedMessage(msg);
      return msg.ack();
    }
    else if (msg.getPayload().getField1().equals("fail")) {
      wrappedFailed.set(true);
      manager.<MockPayload>getReceiver(WRAPPED_MESSAGE).receiveWrappedMessage(msg);
      throw new QuietRuntimeException("failed");
    }
    else {
      manager.<MockPayload>getReceiver(WRAPPED_MESSAGE).receiveWrappedMessage(msg);
      return CompletableFuture.completedFuture(null);
    }
  }
//
//  private final AtomicInteger acked = new AtomicInteger();
//  private final AtomicBoolean outgoingWrappedFailed = new AtomicBoolean();
//
//  public AtomicInteger getAcked() {
//    return acked;
//  }
//
//  public AtomicBoolean getOutgoingWrappedFailed() {
//    return outgoingWrappedFailed;
//  }
//
//  @Incoming(OUTGOING_WRAPPED)
//  public CompletionStage<Message<Void>> handleOutgoingWrapped(MockPayload msg) {
//    if (msg.getField1().equals("fail") && !outgoingWrappedFailed.getAndSet(true)) {
//      manager.getReceiver(OUTGOING_WRAPPED).receiveMessage(msg);
//      throw new QuietRuntimeException("failed");
//    }
//    else {
//      manager.getReceiver(OUTGOING_WRAPPED).receiveMessage(msg);
//      return CompletableFuture.completedFuture(Message.of(null, () -> {
//        acked.incrementAndGet();
//        return CompletableFuture.completedFuture(null);
//      }));
//    }
//  }
//
//  private final AtomicBoolean incomingOutgoingWrappedFailed = new AtomicBoolean();
//
//  public AtomicBoolean getIncomingOutgoingWrappedFailed() {
//    return incomingOutgoingWrappedFailed;
//  }
//
//  @Incoming(INCOMING_OUTGOING_WRAPPED)
//  public CompletionStage<Message<Void>> handleIncomingOutgoingWrapped(Message<MockPayload> msg) {
//    System.out.println("handleIncomingOutgoingWrapped");
//    if (msg.getPayload().getField1().equals("acknowledged") || incomingOutgoingWrappedFailed.get()) {
//      manager.<MockPayload>getReceiver(INCOMING_OUTGOING_WRAPPED).receiveWrappedMessage(msg);
//      return CompletableFuture.completedFuture(Message.of(null, msg::ack));
//    }
//    else if (msg.getPayload().getField1().equals("fail")) {
//      incomingOutgoingWrappedFailed.set(true);
//      manager.<MockPayload>getReceiver(INCOMING_OUTGOING_WRAPPED).receiveWrappedMessage(msg);
//      throw new QuietRuntimeException("failed");
//    }
//    else {
//      manager.<MockPayload>getReceiver(INCOMING_OUTGOING_WRAPPED).receiveWrappedMessage(msg);
//      // Note - not transferring the ack.
//      return CompletableFuture.completedFuture(Message.of(null));
//    }
//  }
}
