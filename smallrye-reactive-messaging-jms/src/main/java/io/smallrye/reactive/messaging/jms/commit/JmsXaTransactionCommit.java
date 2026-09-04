package io.smallrye.reactive.messaging.jms.commit;

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

public class JmsXaTransactionCommit implements JmsCommitHandler {

    private final Executor executor;
    private final TransactionManager tm;

    public JmsXaTransactionCommit(Executor executor, TransactionManager tm) {
        this.executor = executor;
        this.tm = tm;
    }

    @Override
    public <T> Uni<Void> handle(IncomingJmsMessage<T> message, Metadata metadata) {
        return Uni.createFrom().voidItem()
                .invoke(() -> {
                    try {
                        JmsXaTransactionMetadata txMeta = message.getMetadata(JmsXaTransactionMetadata.class)
                                .orElseThrow(() -> new IllegalStateException("No XA transaction metadata on message"));
                        if (tm.getStatus() != Status.STATUS_ACTIVE) {
                            txMeta.resume();
                        }
                        tm.commit();
                        log.debugf("XA transaction committed: tx=%s", txMeta.transaction());
                    } catch (Exception e) {
                        throw ex.jmsTransactionFailure("commit", e);
                    } finally {
                        message.getMetadata(JmsSessionContext.class)
                                .ifPresent(ctx -> ctx.jmsContext().close());
                    }
                })
                .runSubscriptionOn(executor)
                .emitOn(message::runOnMessageContext);
    }
}
