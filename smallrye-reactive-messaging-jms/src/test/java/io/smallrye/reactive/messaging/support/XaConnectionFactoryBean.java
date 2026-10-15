package io.smallrye.reactive.messaging.support;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Disposes;
import jakarta.enterprise.inject.Produces;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.XAConnectionFactory;
import jakarta.transaction.TransactionManager;

import org.apache.activemq.artemis.jms.client.ActiveMQXAConnectionFactory;

@ApplicationScoped
public class XaConnectionFactoryBean {

    @Produces
    ConnectionFactory connectionFactory() {
        return new ActiveMQXAConnectionFactory("tcp://localhost:61616", null, null);
    }

    @Produces
    XAConnectionFactory xaConnectionFactory() {
        return new ActiveMQXAConnectionFactory("tcp://localhost:61616", null, null);
    }

    void closeXaConnectionFactory(@Disposes XAConnectionFactory factory) {
        ((ActiveMQXAConnectionFactory) factory).close();
    }

    @Produces
    TransactionManager transactionManager() {
        return com.arjuna.ats.jta.TransactionManager.transactionManager();
    }
}
