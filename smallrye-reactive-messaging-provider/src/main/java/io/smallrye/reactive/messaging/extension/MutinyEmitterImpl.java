package io.smallrye.reactive.messaging.extension;

import static io.smallrye.reactive.messaging.i18n.ProviderExceptions.ex;

import java.util.concurrent.CompletableFuture;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.Cancellable;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.i18n.ProviderLogging;

public class MutinyEmitterImpl<T> extends AbstractEmitter<T> implements MutinyEmitter<T> {
    public MutinyEmitterImpl(EmitterConfiguration config, long defaultBufferSize) {
        super(config, defaultBufferSize);
    }

    @Override
    public Uni<Void> send(T payload) {
        if (payload == null) {
            throw ex.illegalArgumentForNullValue();
        }

        return Uni.createFrom().emitter(e -> emit(Message.of(payload, Metadata.empty(), () -> {
            e.complete(null);
            return CompletableFuture.completedFuture(null);
        },
                reason -> {
                    e.fail(reason);
                    return CompletableFuture.completedFuture(null);
                })));
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
    public <M extends Message<? extends T>> Uni<Void> send(M msg) {
        if (msg == null) {
            throw ex.illegalArgumentForNullValue();
        }
        return Uni.createFrom().emitter(e -> {
            emit(Message.of(msg.getPayload(), msg.getMetadata(), () -> {
                msg.ack();
                e.complete(null);
                return CompletableFuture.completedFuture(null);
            },
                    reason -> {
                        msg.nack(reason);
                        e.fail(reason);
                        return CompletableFuture.completedFuture(null);
                    }));
        });
    }

    @Override
    public <M extends Message<? extends T>> void sendAndAwait(M msg) {
        send(msg).await().indefinitely();
    }

    @Override
    public <M extends Message<? extends T>> Cancellable sendAndForget(M msg) {
        return send(msg).subscribe().with(x -> {
        }, ProviderLogging.log::failureEmittingMessage);
    }
}
