package io.smallrye.reactive.messaging.kafka.commit;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.common.errors.InterruptException;

import io.smallrye.reactive.messaging.providers.helpers.VertxContext;
import io.vertx.mutiny.core.Context;
import io.vertx.mutiny.core.Vertx;

/**
 * A class holding a vert.x context to make sure methods are always run from the same one.
 */
public class ContextHolder {

    protected final Vertx vertx;
    private final int timeout;
    protected volatile Context context;

    public ContextHolder(Vertx vertx, int defaultTimeout) {
        this.vertx = vertx;
        this.timeout = defaultTimeout;
    }

    public void capture(Context context) {
        this.context = context;
    }

    public void capture(io.vertx.core.Context context) {
        this.context = Context.newInstance(context);
    }

    public Context getContext() {
        return context;
    }

    public int getTimeoutInMillis() {
        return timeout;
    }

    public void runOnContext(Runnable runnable) {
        // Directly run the task if current thread is an event loop thread and have the captured context
        // Otherwise run the task on event loop
        VertxContext.runOnEventLoopContext(context.getDelegate(), runnable);
    }

    public <T> T runOnContextAndAwait(Callable<T> action) {
        try {
            return VertxContext.callOnContext(context.getDelegate(), action)
                    .toCompletableFuture()
                    .get(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            // The InterruptException reset the interruption flag.
            throw new InterruptException(e);
        } catch (ExecutionException | TimeoutException e) {
            throw new CompletionException(e);
        }
    }

}
