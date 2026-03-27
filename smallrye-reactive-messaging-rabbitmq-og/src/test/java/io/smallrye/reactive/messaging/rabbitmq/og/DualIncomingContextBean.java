package io.smallrye.reactive.messaging.rabbitmq.og;

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
public class DualIncomingContextBean {

    private final CountDownLatch latch1 = new CountDownLatch(1);
    private final CountDownLatch latch2 = new CountDownLatch(1);
    private final AtomicReference<Context> context1 = new AtomicReference<>();
    private final AtomicReference<Context> context2 = new AtomicReference<>();
    private final AtomicReference<Boolean> eventLoop1 = new AtomicReference<>();
    private final AtomicReference<Boolean> eventLoop2 = new AtomicReference<>();

    @Incoming("data1")
    public Uni<Void> consume1(Message<Integer> message) {
        message.getMetadata(LocalContextMetadata.class).ifPresent(metadata -> {
            Context context = metadata.context();
            context1.set(context);
            eventLoop1.set(context.isEventLoopContext());
        });
        latch1.countDown();
        return Uni.createFrom().voidItem();
    }

    @Incoming("data2")
    public Uni<Void> consume2(Message<Integer> message) {
        message.getMetadata(LocalContextMetadata.class).ifPresent(metadata -> {
            Context context = metadata.context();
            context2.set(context);
            eventLoop2.set(context.isEventLoopContext());
        });
        latch2.countDown();
        return Uni.createFrom().voidItem();
    }

    public boolean awaitMessages(long timeout, TimeUnit unit) throws InterruptedException {
        return latch1.await(timeout, unit) && latch2.await(timeout, unit);
    }

    public Context getContext1() {
        return context1.get();
    }

    public Context getContext2() {
        return context2.get();
    }

    public boolean isEventLoop1() {
        Boolean value = eventLoop1.get();
        return value != null && value;
    }

    public boolean isEventLoop2() {
        Boolean value = eventLoop2.get();
        return value != null && value;
    }
}
