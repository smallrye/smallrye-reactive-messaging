package io.smallrye.reactive.messaging.tck.incoming;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Incoming;

import io.smallrye.reactive.messaging.tck.MessagingManager;
import io.smallrye.reactive.messaging.tck.MockPayload;
import io.smallrye.reactive.messaging.tck.QuietRuntimeException;

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
        } else {
            manager.getReceiver(SYNC_FAILING).receiveMessage(payload);
            return CompletableFuture.completedFuture(null);
        }
    }

    private final AtomicBoolean asyncFailed = new AtomicBoolean();

    public AtomicBoolean getAsyncFailed() {
        return asyncFailed;
    }
}
