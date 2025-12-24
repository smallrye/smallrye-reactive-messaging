package io.smallrye.reactive.messaging.rabbitmq;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.providers.locals.LocalContextMetadata;
import io.vertx.core.Context;

@ApplicationScoped
public class IncomingContextBean {

    private final CountDownLatch latch = new CountDownLatch(1);
    private final AtomicReference<Context> messageContext = new AtomicReference<>();
    private final AtomicReference<Boolean> eventLoopContext = new AtomicReference<>();

    @Incoming("data")
    public Uni<Void> consume(Message<Integer> message) {
        message.getMetadata(LocalContextMetadata.class).ifPresent(metadata -> {
            Context context = metadata.context();
            messageContext.set(context);
            eventLoopContext.set(context.isEventLoopContext());
        });
        latch.countDown();
        return Uni.createFrom().voidItem();
    }

    public boolean awaitMessage(long timeout, TimeUnit unit) throws InterruptedException {
        return latch.await(timeout, unit);
    }

    public Context getMessageContext() {
        return messageContext.get();
    }

    public boolean isEventLoopContext() {
        Boolean value = eventLoopContext.get();
        return value != null && value;
    }
}
