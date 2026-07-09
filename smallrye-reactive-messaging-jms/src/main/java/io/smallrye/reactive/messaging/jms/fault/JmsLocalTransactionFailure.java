package io.smallrye.reactive.messaging.jms.fault;

import static io.smallrye.reactive.messaging.jms.i18n.JmsExceptions.ex;

import java.util.concurrent.Executor;

import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.jms.IncomingJmsMessage;
import io.smallrye.reactive.messaging.jms.JmsSessionContext;

public class JmsLocalTransactionFailure implements JmsFailureHandler {

    private final Executor executor;
    private final JmsFailureHandler delegate;
    private final Runnable onProcessed;

    public JmsLocalTransactionFailure(Executor executor, JmsFailureHandler delegate, Runnable onProcessed) {
        this.executor = executor;
        this.delegate = delegate;
        this.onProcessed = onProcessed;
    }

    @Override
    public <T> Uni<Void> handle(IncomingJmsMessage<T> message, Throwable reason, Metadata metadata) {
        return Uni.createFrom().voidItem()
                .invoke(() -> {
                    try {
                        message.getMetadata(JmsSessionContext.class)
                                .orElseThrow(() -> new IllegalStateException("No JmsSessionContext on message"))
                                .jmsContext().rollback();
                    } catch (Exception e) {
                        throw ex.jmsTransactionFailure("rollback", e);
                    } finally {
                        onProcessed.run();
                    }
                })
                .runSubscriptionOn(executor)
                .chain(() -> delegate.handle(message, reason, metadata));
    }

    @Override
    public void close() {
        delegate.close();
    }
}
