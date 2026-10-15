package io.smallrye.reactive.messaging.jms.commit;

import static io.smallrye.reactive.messaging.jms.i18n.JmsExceptions.ex;
import static io.smallrye.reactive.messaging.jms.i18n.JmsLogging.log;

import java.util.concurrent.Executor;

import jakarta.transaction.Status;

import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.jms.IncomingJmsMessage;
import io.smallrye.reactive.messaging.jms.JmsXaTransactionMetadata;

public class JmsXaTransactionCommit implements JmsCommitHandler {

    private final Executor executor;

    public JmsXaTransactionCommit(Executor executor) {
        this.executor = executor;
    }

    @Override
    public <T> Uni<Void> handle(IncomingJmsMessage<T> message, Metadata metadata) {
        return Uni.createFrom().voidItem()
                .invoke(() -> {
                    JmsXaTransactionMetadata txMeta = message.getMetadata(JmsXaTransactionMetadata.class)
                            .orElseThrow(() -> new IllegalStateException("No XA transaction metadata on message"));
                    // Check the message's own transaction, not the one on the executor thread.
                    // The JMS context is closed by the Synchronization registered by the poller.
                    int status = txMeta.status();
                    switch (status) {
                        case Status.STATUS_ACTIVE -> {
                            txMeta.commit();
                            log.debugf("XA transaction committed: tx=%s", txMeta.transaction());
                        }
                        case Status.STATUS_COMMITTED ->
                            log.debugf("XA transaction already committed: tx=%s", txMeta.transaction());
                        case Status.STATUS_ROLLEDBACK ->
                            log.debugf("XA transaction already rolled back (nacked?): tx=%s", txMeta.transaction());
                        case Status.STATUS_MARKED_ROLLBACK -> {
                            // rollback-only (e.g. set by the application): nothing can be committed
                            try {
                                txMeta.rollback();
                                log.warnf("XA transaction marked rollback-only, rolled back: tx=%s",
                                        txMeta.transaction());
                            } catch (RuntimeException e) {
                                log.warnf(e, "XA transaction rollback failed (was marked rollback-only): tx=%s",
                                        txMeta.transaction());
                            }
                        }
                        default -> throw ex.jmsTransactionFailure("commit",
                                new IllegalStateException("XA transaction is no longer active (status=" + status +
                                        "), it may have timed out"));
                    }
                })
                .runSubscriptionOn(executor)
                .emitOn(message::runOnMessageContext);
    }
}
