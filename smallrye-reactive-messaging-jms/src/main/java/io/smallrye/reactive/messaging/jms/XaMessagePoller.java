package io.smallrye.reactive.messaging.jms;

import java.util.List;

import jakarta.jms.Destination;
import jakarta.jms.JMSConsumer;
import jakarta.jms.Topic;
import jakarta.jms.XAConnectionFactory;
import jakarta.jms.XAJMSContext;
import jakarta.transaction.Transaction;
import jakarta.transaction.TransactionManager;

import org.eclipse.microprofile.reactive.messaging.Message;

class XaMessagePoller implements JmsMessagePoller {

    private final XAConnectionFactory xaConnectionFactory;
    private final TransactionManager transactionManager;
    private final String destinationName;
    private final String type;
    private final String selector;
    private final boolean nolocal;
    private final boolean durable;
    private final long receiveTimeoutMs;

    XaMessagePoller(XAConnectionFactory xaConnectionFactory, TransactionManager transactionManager,
            JmsConnectorIncomingConfiguration config) {
        this.xaConnectionFactory = xaConnectionFactory;
        this.transactionManager = transactionManager;
        this.destinationName = config.getDestination().orElseGet(config::getChannel);
        this.type = config.getDestinationType();
        this.selector = config.getSelector().orElse(null);
        this.nolocal = config.getNoLocal();
        this.durable = config.getDurable();
        this.receiveTimeoutMs = config.getReceiveTimeout();
    }

    @Override
    public Message<jakarta.jms.Message> poll() throws Exception {
        XAJMSContext xaContext = xaConnectionFactory.createXAContext();
        try {
            Destination dest = "topic".equalsIgnoreCase(type)
                    ? xaContext.createTopic(destinationName)
                    : xaContext.createQueue(destinationName);
            JMSConsumer xaConsumer = durable && dest instanceof Topic topic
                    ? xaContext.createDurableConsumer(topic, destinationName, selector, nolocal)
                    : xaContext.createConsumer(dest, selector, nolocal);

            transactionManager.begin();
            try {
                transactionManager.getTransaction().enlistResource(xaContext.getXAResource());

                jakarta.jms.Message received = xaConsumer.receive(receiveTimeoutMs);
                xaConsumer.close();

                if (received == null) {
                    transactionManager.rollback();
                    xaContext.close();
                    return null;
                }

                Transaction suspended = transactionManager.suspend();
                return Message.of(received, List.of(
                        new JmsSessionContext(xaContext, JmsTransactionMode.XA),
                        new JmsXaTransactionMetadata(suspended, transactionManager)));
            } catch (Exception e) {
                transactionManager.rollback();
                throw e;
            }
        } catch (Exception e) {
            xaContext.close();
            throw e;
        }
    }
}
