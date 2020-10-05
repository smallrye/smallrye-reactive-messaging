package io.smallrye.reactive.messaging.kafka.commit;

import java.lang.reflect.Field;

import io.vertx.kafka.client.consumer.KafkaReadStream;
import io.vertx.kafka.client.consumer.impl.KafkaReadStreamImpl;
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

    public void capture(KafkaReadStream<?, ?> stream) {
        if (!(stream instanceof KafkaReadStreamImpl)) {
            throw new IllegalArgumentException("Cannot capture the context - not a KafkaReadStreamImpl");
        } else {
            try {
                Field field = KafkaReadStreamImpl.class.getDeclaredField("context");
                field.setAccessible(true);
                context = new Context((io.vertx.core.Context) field.get(stream));
            } catch (Exception e) {
                throw new IllegalArgumentException("Cannot capture the context", e);
            }
        }
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

}
