package io.smallrye.reactive.messaging.extension;

import static io.smallrye.reactive.messaging.i18n.ProviderExceptions.ex;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

/**
 * Implementation of the emitter pattern.
 *
 * @param <T> the type of payload sent by the emitter.
 */
public class EmitterImpl<T> extends AbstractEmitter<T> implements Emitter<T> {

    public EmitterImpl(EmitterConfiguration config, long defaultBufferSize) {
        super(config, defaultBufferSize);
    }

    @Override
    public synchronized CompletionStage<Void> send(T payload) {
        if (payload == null) {
            throw ex.illegalArgumentForNullValue();
        }
        CompletableFuture<Void> future = new CompletableFuture<>();
        emit(Message.of(payload, Metadata.empty(), () -> {
            future.complete(null);
            return CompletableFuture.completedFuture(null);
        },
                reason -> {
                    future.completeExceptionally(reason);
                    return CompletableFuture.completedFuture(null);
                }));
        return future;
    }

    @Override
    public synchronized <M extends Message<? extends T>> void send(M msg) {
        if (msg == null) {
            throw ex.illegalArgumentForNullValue();
        }
        emit(msg);
    }

}
