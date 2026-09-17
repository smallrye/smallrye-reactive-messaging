package io.smallrye.reactive.messaging.jms.fault;

import static io.smallrye.reactive.messaging.jms.i18n.JmsExceptions.ex;
import static io.smallrye.reactive.messaging.jms.i18n.JmsLogging.log;

import java.util.concurrent.Executor;

import jakarta.transaction.Status;
import jakarta.transaction.TransactionManager;

import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.jms.IncomingJmsMessage;
import io.smallrye.reactive.messaging.jms.JmsSessionContext;
import io.smallrye.reactive.messaging.jms.JmsXaTransactionMetadata;

public class JmsXaTransactionFailure implements JmsFailureHandler {

    private final Executor executor;
    private final TransactionManager tm;
    private final JmsFailureHandler delegate;

    public JmsXaTransactionFailure(Executor executor, TransactionManager tm, JmsFailureHandler delegate) {
        this.executor = executor;
        this.tm = tm;
        this.delegate = delegate;
    }

    @Override
    public <T> Uni<Void> handle(IncomingJmsMessage<T> message, Throwable reason, Metadata metadata) {
        return Uni.createFrom().voidItem()
                .invoke(() -> {
                    try {
                        JmsXaTransactionMetadata txMeta = message.getMetadata(JmsXaTransactionMetadata.class)
                                .orElseThrow(() -> new IllegalStateException("No XA transaction metadata on message"));
                        if (tm.getStatus() != Status.STATUS_ACTIVE) {
                            txMeta.resume();
                        }
                        tm.rollback();
                        log.debugf("XA transaction rollback: tx=%s, reason=%s", txMeta.transaction(), reason.getMessage());
                    } catch (Exception e) {
                        throw ex.jmsTransactionFailure("rollback", e);
                    } finally {
                        message.getMetadata(JmsSessionContext.class)
                                .ifPresent(ctx -> ctx.jmsContext().close());
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
