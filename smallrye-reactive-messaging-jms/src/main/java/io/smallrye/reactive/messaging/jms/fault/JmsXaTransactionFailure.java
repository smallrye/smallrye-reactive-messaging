package io.smallrye.reactive.messaging.jms.fault;

import static io.smallrye.reactive.messaging.jms.i18n.JmsLogging.log;

import java.util.concurrent.Executor;

import jakarta.transaction.Status;

import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.jms.IncomingJmsMessage;
import io.smallrye.reactive.messaging.jms.JmsXaTransactionMetadata;

public class JmsXaTransactionFailure implements JmsFailureHandler {

    private final Executor executor;
    private final JmsFailureHandler delegate;

    public JmsXaTransactionFailure(Executor executor, JmsFailureHandler delegate) {
        this.executor = executor;
        this.delegate = delegate;
    }

    @Override
    public <T> Uni<Void> handle(IncomingJmsMessage<T> message, Throwable reason, Metadata metadata) {
        return Uni.createFrom().voidItem()
                .invoke(() -> {
                    JmsXaTransactionMetadata txMeta = message.getMetadata(JmsXaTransactionMetadata.class)
                            .orElseThrow(() -> new IllegalStateException("No XA transaction metadata on message"));
                    // The transaction may already be gone (e.g. timed out), in which case the broker
                    // has already rolled back, and the JMS context is closed by the poller's Synchronization.
                    int status = txMeta.status();
                    if (status == Status.STATUS_ACTIVE || status == Status.STATUS_MARKED_ROLLBACK) {
                        try {
                            txMeta.rollback();
                            log.debugf("XA transaction rollback: tx=%s, reason=%s", txMeta.transaction(),
                                    reason.getMessage());
                        } catch (RuntimeException e) {
                            e.addSuppressed(reason);
                            log.warnf(e, "XA transaction rollback failed: tx=%s", txMeta.transaction());
                        }
                    } else {
                        log.debugf("XA transaction already completed (status=%d): tx=%s", status,
                                txMeta.transaction());
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
