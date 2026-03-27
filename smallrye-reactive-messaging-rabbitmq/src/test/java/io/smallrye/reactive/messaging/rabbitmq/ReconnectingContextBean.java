package io.smallrye.reactive.messaging.rabbitmq;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.providers.locals.LocalContextMetadata;
import io.vertx.core.Context;

@ApplicationScoped
public class ReconnectingContextBean {

    private final CopyOnWriteArrayList<Context> contexts = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<Boolean> eventLoopFlags = new CopyOnWriteArrayList<>();
    private volatile CountDownLatch latch = new CountDownLatch(1);

    @Incoming("data")
    public Uni<Void> consume(Message<Integer> message) {
        message.getMetadata(LocalContextMetadata.class).ifPresent(metadata -> {
            Context context = metadata.context();
            contexts.add(context);
            eventLoopFlags.add(context.isEventLoopContext());
        });
        latch.countDown();
        return Uni.createFrom().voidItem();
    }

    public void setExpectedMessages(int count) {
        latch = new CountDownLatch(count);
    }

    public boolean awaitMessages(long timeout, TimeUnit unit) throws InterruptedException {
        return latch.await(timeout, unit);
    }

    public List<Context> getContexts() {
        return contexts;
    }

    public List<Boolean> getEventLoopFlags() {
        return eventLoopFlags;
    }
}
