package io.smallrye.reactive.messaging.providers.extension;

import static io.smallrye.reactive.messaging.providers.i18n.ProviderExceptions.ex;

import java.util.concurrent.Flow.Publisher;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.reactive.messaging.MessagePublisherProvider;
import io.smallrye.reactive.messaging.annotations.Emitter;

/**
 * Implementation of the old (legacy) Emitter interface.
 * This implementation delegates to the new implementation.
 *
 * @param <T> the type of payload or message
 */
@SuppressWarnings("deprecation")
public class LegacyEmitterImpl<T> implements Emitter<T>, MessagePublisherProvider<T> {

    private final EmitterImpl<T> delegate;

    LegacyEmitterImpl(EmitterImpl<T> delegate) {
        this.delegate = delegate;
    }

    @Override
    public Publisher<Message<? extends T>> getPublisher() {
        return delegate.getPublisher();
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
