package io.smallrye.reactive.messaging.kafka.commit;

import java.util.concurrent.*;

import org.apache.kafka.common.errors.InterruptException;

import io.vertx.core.Context;
import io.vertx.core.Vertx;

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

    public Context getContext() {
        return context;
    }

    public void runOnContext(Runnable runnable) {
        if (Vertx.currentContext() == context) {
            runnable.run();
        } else {
            context.runOnContext(x -> runnable.run());
        }
    }

    public <T> T runOnContextAndAwait(Callable<T> action) {
        FutureTask<T> task = new FutureTask<>(action);
        context.runOnContext(x -> task.run());

        try {
            return task.get(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            // The InterruptException reset the interruption flag.
            throw new InterruptException(e);
        } catch (ExecutionException | TimeoutException e) {
            throw new CompletionException(e);
        }
    }

}
