package io.smallrye.reactive.messaging.jms;

import static io.smallrye.reactive.messaging.jms.i18n.JmsExceptions.ex;

import jakarta.transaction.Status;
import jakarta.transaction.Transaction;
import jakarta.transaction.TransactionManager;

import io.smallrye.common.annotation.Experimental;

/**
 * Message metadata carrying a suspended XA transaction, available on messages
 * received with {@code transaction-mode=xa}.
 * <p>
 * The transaction is suspended after receive so it can be resumed on the processing thread.
 * Use {@link #withTransaction()} to associate the transaction with the current thread for the duration of a try-with-resources
 * block:
 *
 * <pre>{@code
 * @Incoming("orders")
 * public void process(int payload, JmsXaTransactionMetadata xa) {
 *     try (var scope = xa.withTransaction()) {
 *         entityManager.persist(toEntity(payload));
 *     }
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

    /**
     * @return the status of the XA transaction carried by this metadata, as defined by {@link Status}.
     *         Unlike {@link TransactionManager#getStatus()}, this does not depend on the transaction
     *         associated with the current thread.
     */
    public int status() {
        try {
            return transaction.getStatus();
        } catch (Exception e) {
            throw ex.jmsTransactionFailure("get status of", e);
        }
    }

    /**
     * Associates this transaction with the current thread and returns a scope that
     * suspends it on {@link TransactionScope#close()}.
     *
     * <pre>{@code
     * try (var scope = xa.withTransaction()) {
     *     entityManager.persist(toEntity(msg.getPayload()));
     * }
     * }</pre>
     *
     * @return a scope that must be closed to suspend the transaction from the thread
     */
    public TransactionScope withTransaction() {
        if (isCurrent()) {
            return TransactionScope.NO_OP;
        }
        resume();
        return this::suspend;
    }

    @FunctionalInterface
    public interface TransactionScope extends AutoCloseable {
        TransactionScope NO_OP = () -> {
        };

        @Override
        void close();
    }

    /**
     * Commits this transaction, whichever thread it is currently associated with.
     * <p>
     * The transaction must be {@link Status#STATUS_ACTIVE active}.
     */
    public void commit() {
        try (var scope = withTransaction()) {
            transactionManager.commit();
        } catch (Exception e) {
            throw ex.jmsTransactionFailure("commit", e);
        }
    }

    /**
     * Rolls back this transaction, whichever thread it is currently associated with.
     */
    public void rollback() {
        try (var scope = withTransaction()) {
            transactionManager.rollback();
        } catch (Exception e) {
            throw ex.jmsTransactionFailure("rollback", e);
        }
    }

    private boolean isCurrent() {
        try {
            return transactionManager.getTransaction() == transaction;
        } catch (Exception e) {
            return false;
        }
    }

}
