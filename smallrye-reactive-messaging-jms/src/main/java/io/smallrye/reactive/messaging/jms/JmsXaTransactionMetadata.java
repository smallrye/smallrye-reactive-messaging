package io.smallrye.reactive.messaging.jms;

import static io.smallrye.reactive.messaging.jms.i18n.JmsExceptions.ex;

import jakarta.transaction.Transaction;
import jakarta.transaction.TransactionManager;

import io.smallrye.common.annotation.Experimental;

/**
 * Message metadata carrying a suspended XA transaction, available on messages
 * received with {@code transaction-mode=xa}.
 * <p>
 * The transaction is suspended after receive so it can be resumed on the
 * processing thread. Container integrations (e.g., Quarkus) typically provide
 * a custom invoker that calls {@link #resume()} before the {@code @Incoming}
 * method and {@link #suspend()} after it returns. Without a container invoker,
 * the application must manage the lifecycle manually:
 *
 * <pre>{@code
 * @Incoming("orders")
 * public CompletionStage<Void> process(IncomingJmsMessage<?> msg) {
 *     JmsXaTransactionMetadata xa = msg.getMetadata(JmsXaTransactionMetadata.class)
 *             .orElseThrow();
 *     xa.resume();
 *     try {
 *         // database and JMS participate in the same XA transaction
 *         entityManager.persist(toEntity(msg.getPayload()));
 *     } finally {
 *         xa.suspend();
 *     }
 *     return msg.ack(); // commits the XA transaction
 * }
 * }</pre>
 */
@Experimental("Experimental API")
public class JmsXaTransactionMetadata {

    private final Transaction transaction;
    private final TransactionManager transactionManager;

    JmsXaTransactionMetadata(Transaction transaction, TransactionManager transactionManager) {
        this.transaction = transaction;
        this.transactionManager = transactionManager;
    }

    /**
     * @return the suspended XA transaction
     */
    public Transaction transaction() {
        return transaction;
    }

    /**
     * Resumes the XA transaction on the current thread.
     * Must be called on the thread that will perform transactional work.
     */
    public void resume() {
        try {
            transactionManager.resume(transaction);
        } catch (Exception e) {
            throw ex.jmsTransactionFailure("resume", e);
        }
    }

    /**
     * Suspends the XA transaction from the current thread.
     * Should be called after transactional work completes, before returning
     * control to the reactive pipeline.
     *
     * @return the suspended transaction
     */
    public Transaction suspend() {
        try {
            return transactionManager.suspend();
        } catch (Exception e) {
            throw ex.jmsTransactionFailure("suspend", e);
        }
    }
}
