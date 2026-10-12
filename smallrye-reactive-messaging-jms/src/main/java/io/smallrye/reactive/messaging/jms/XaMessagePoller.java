package io.smallrye.reactive.messaging.jms;

import static io.smallrye.reactive.messaging.jms.i18n.JmsLogging.log;

import java.util.List;
import java.util.concurrent.Executor;

import jakarta.jms.Destination;
import jakarta.jms.JMSConsumer;
import jakarta.jms.Topic;
import jakarta.jms.XAConnectionFactory;
import jakarta.jms.XAJMSContext;
import jakarta.transaction.Synchronization;
import jakarta.transaction.Transaction;
import jakarta.transaction.TransactionManager;

import org.eclipse.microprofile.reactive.messaging.Message;

/**
 * Polls a JMS destination in an XA transaction, returning the message and a suspended transaction.
 * <p>
 * The XA transaction is started before the receive and suspended after, so that the message can be processed in the context of
 * the transaction. The transaction is resumed on the processing thread by the invoker.
 * <p>
 * The XA JMS context is created for the duration of the transaction and is not closed by the poller.
 * The message processing code is responsible for closing it,
 * by registering a {@link jakarta.transaction.Synchronization} on the transaction that closes the context when the transaction
 * completes (commit, rollback or timeout).
 * The commit and failure handlers do not close the context.
 */
class XaMessagePoller implements JmsMessagePoller {

    private final XAConnectionFactory xaConnectionFactory;
    private final TransactionManager transactionManager;
    private final Executor closeExecutor;
    private final String destinationName;
    private final String type;
    private final String selector;
    private final boolean nolocal;
    private final boolean durable;
    private final long receiveTimeoutMs;

    XaMessagePoller(XAConnectionFactory xaConnectionFactory, TransactionManager transactionManager,
            JmsConnectorIncomingConfiguration config, Executor closeExecutor) {
        this.xaConnectionFactory = xaConnectionFactory;
        this.transactionManager = transactionManager;
        this.closeExecutor = closeExecutor;
        this.destinationName = config.getDestination().orElseGet(config::getChannel);
        this.type = config.getDestinationType();
        this.selector = config.getSelector().orElse(null);
        this.nolocal = config.getNoLocal();
        this.durable = config.getDurable();
        if (config.getReceiveTimeout() <= 0) {
            // an XA transaction is open during the receive: blocking forever would exceed the transaction timeout
            throw new IllegalArgumentException(
                    "receive-timeout must be greater than 0 with transaction-mode=xa (channel `" + config.getChannel()
                            + "`)");
        }
        this.receiveTimeoutMs = config.getReceiveTimeout();
    }

    @Override
    public Message<jakarta.jms.Message> poll() throws Exception {
        XAJMSContext xaContext = xaConnectionFactory.createXAContext();
        Transaction tx = null;
        try {
            Destination dest = "topic".equalsIgnoreCase(type)
                    ? xaContext.createTopic(destinationName)
                    : xaContext.createQueue(destinationName);
            JMSConsumer xaConsumer = durable && dest instanceof Topic topic
                    ? xaContext.createDurableConsumer(topic, destinationName, selector, nolocal)
                    : xaContext.createConsumer(dest, selector, nolocal);

            transactionManager.begin();
            tx = transactionManager.getTransaction();
            tx.enlistResource(xaContext.getXAResource());

            jakarta.jms.Message received = xaConsumer.receive(receiveTimeoutMs);
            xaConsumer.close();

            if (received == null) {
                // Nothing to hand out: tear everything down right here on the polling thread
                try {
                    transactionManager.rollback();
                } catch (Exception rollbackEx) {
                    log.debugf(rollbackEx, "Error rolling back empty-receive XA transaction");
                }
                closeContext(xaContext);
                return null;
            }

            // The context is handed to the message: it lives as long as the transaction, whatever its outcome
            // (commit, rollback, timeout), even if the message is never acked nor nacked.
            tx.registerSynchronization(new ContextCloser(xaContext, closeExecutor));

            Transaction suspended = transactionManager.suspend();
            return Message.of(received, List.of(
                    new JmsSessionContext(xaContext, JmsTransactionMode.XA),
                    new JmsXaTransactionMetadata(suspended, transactionManager)));
        } catch (Exception e) {
            rollbackQuietly(tx, e);
            closeContextQuietly(xaContext, e);
            throw e;
        }
    }

    private void rollbackQuietly(Transaction tx, Exception cause) {
        if (tx == null) {
            return;
        }
        try {
            if (transactionManager.getTransaction() == tx) {
                transactionManager.rollback();
            } else {
                tx.rollback();
            }
        } catch (Exception e) {
            cause.addSuppressed(e);
        }
        // never leave a failed transaction associated with the polling thread
        try {
            if (transactionManager.getTransaction() == tx) {
                transactionManager.suspend();
            }
        } catch (Exception e) {
            cause.addSuppressed(e);
        }
    }

    private static void closeContextQuietly(XAJMSContext xaContext, Exception cause) {
        try {
            xaContext.close();
        } catch (Exception e) {
            cause.addSuppressed(e);
        }
    }

    private static void closeContext(XAJMSContext xaContext) {
        try {
            xaContext.close();
        } catch (Exception e) {
            log.debugf(e, "Error closing XA JMS context");
        }
    }

    private record ContextCloser(XAJMSContext xaContext, Executor closeExecutor) implements Synchronization {
        @Override
        public void beforeCompletion() {
        }

        @Override
        public void afterCompletion(int status) {
            // On timeout this runs on the transaction reaper thread, which must not block on a network call
            try {
                closeExecutor.execute(() -> close(status));
            } catch (RuntimeException rejected) {
                log.debugf(rejected, "Could not submit XA JMS context close to executor, the broker will clean up");
            }
        }

        private void close(int status) {
            try {
                xaContext.close();
            } catch (Exception e) {
                // already closed or connection lost: the broker rolls back the branch
                log.debugf(e, "Error closing XA JMS context after transaction completion (status=%d)", status);
            }
        }
    }
}
