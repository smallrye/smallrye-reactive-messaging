package io.smallrye.reactive.messaging.extension;

import static io.smallrye.reactive.messaging.i18n.ProviderExceptions.ex;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.reactive.messaging.annotations.Emitter;

/**
 * Implementation of the old (legacy) Emitter interface.
 * This implementation delegates to the new implementation.
 *
 * @param <T> the type of payload or message
 */
public class LegacyEmitterImpl<T> implements Emitter<T> {

    private final org.eclipse.microprofile.reactive.messaging.Emitter<T> delegate;

    LegacyEmitterImpl(org.eclipse.microprofile.reactive.messaging.Emitter<T> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void complete() {
        delegate.complete();
    }

    @Override
    public void error(Exception e) {
        delegate.error(e);
    }

    @Override
    public boolean isCancelled() {
        return delegate.isCancelled();
    }

    @Override
    public boolean isRequested() {
        return delegate.hasRequests();
    }

    @Override
    public synchronized Emitter<T> send(T msg) {
        if (msg == null) {
            throw ex.illegalArgumentForNullValue();
        }
        if (msg instanceof Message) {
            delegate.send((Message) msg);
        } else {
            delegate.send(Message.of(msg));
        }
        return this;
    }

}
