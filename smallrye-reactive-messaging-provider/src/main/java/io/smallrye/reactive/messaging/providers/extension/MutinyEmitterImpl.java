package io.smallrye.reactive.messaging.providers.extension;

import static io.smallrye.reactive.messaging.providers.i18n.ProviderExceptions.ex;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.common.annotation.CheckReturnValue;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.Cancellable;
import io.smallrye.reactive.messaging.EmitterConfiguration;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.providers.i18n.ProviderLogging;
import io.smallrye.reactive.messaging.providers.locals.ContextAwareMessage;
import io.vertx.core.Context;
import io.vertx.core.Vertx;

public class MutinyEmitterImpl<T> extends AbstractEmitter<T> implements MutinyEmitter<T> {
    public MutinyEmitterImpl(EmitterConfiguration config, long defaultBufferSize) {
        super(config, defaultBufferSize);
    }

    @Override
    @CheckReturnValue
    public Uni<Void> send(T payload) {
        if (payload == null) {
            throw ex.illegalArgumentForNullValue();
        }

        return sendMessage(Message.of(payload));
    }

    @Override
    public void sendAndAwait(T payload) {
        send(payload).await().indefinitely();
    }

    @Override
    public Cancellable sendAndForget(T payload) {
        return send(payload).subscribe().with(x -> {
        }, ProviderLogging.log::failureEmittingMessage);
    }

    @Override
    public <M extends Message<? extends T>> void send(M msg) {
        sendMessageAndForget(msg);
    }

    @Override
    public <M extends Message<? extends T>> Cancellable sendMessageAndForget(M msg) {
        return sendMessage(msg).subscribe().with(x -> {
            // Do nothing.
        }, ProviderLogging.log::failureEmittingMessage);
    }

    @Override
    public <M extends Message<? extends T>> void sendMessageAndAwait(M msg) {
        sendMessage(msg).await().indefinitely();
    }

    @Override
    @CheckReturnValue
    public <M extends Message<? extends T>> Uni<Void> sendMessage(M msg) {
        if (msg == null) {
            throw ex.illegalArgumentForNullValue();
        }

        // If we are running on a Vert.x I/O thread, we need to capture the context to switch back
        // during the emission.
        Context context = Vertx.currentContext();
        Uni<Void> uni = Uni.createFrom().emitter(e -> {
            try {
                emit(ContextAwareMessage.withContextMetadata((Message<? extends T>) msg)
                        .withAck(() -> {
                            e.complete(null);
                            return msg.ack();
                        })
                        .withNack(t -> {
                            e.fail(t);
                            return msg.nack(t);
                        }));
            } catch (Exception t) {
                // Capture synchronous exception and nack the message.
                msg.nack(t);
                throw t;
            }
        });
        if (context != null) {
            uni = uni.emitOn(runnable -> context.runOnContext(x -> runnable.run()));
        }
        return uni;
    }

}
