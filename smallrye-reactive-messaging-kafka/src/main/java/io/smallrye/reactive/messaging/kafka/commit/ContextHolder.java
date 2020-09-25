package io.smallrye.reactive.messaging.kafka.commit;

import io.vertx.mutiny.core.Context;
import io.vertx.mutiny.core.Vertx;

/**
 * A class holding a vert.x context to make sure methods are always run from the same one.
 */
public class ContextHolder {

    protected final Vertx vertx;
    protected volatile Context context;

    public ContextHolder(Vertx vertx) {
        this.vertx = vertx;
    }

    public Context getContext() {
        Context ctx = this.context;
        if (ctx == null) {
            synchronized (this) {
                ctx = this.context;
                if (ctx == null) {
                    this.context = ctx = vertx.getOrCreateContext();
                }
            }
        }
        return ctx;
    }

    public void runOnContext(Runnable runnable) {
        if (Vertx.currentContext() == getContext()) {
            runnable.run();
        } else {
            getContext().runOnContext(x -> runnable.run());
        }
    }

}
