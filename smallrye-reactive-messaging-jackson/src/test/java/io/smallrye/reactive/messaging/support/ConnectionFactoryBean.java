package io.smallrye.reactive.messaging.support;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.jms.ConnectionFactory;

import org.apache.activemq.artemis.jms.client.ActiveMQJMSConnectionFactory;

@ApplicationScoped
public class ConnectionFactoryBean {

    @Produces
    ConnectionFactory factory() {
        return new ActiveMQJMSConnectionFactory(
                "tcp://localhost:61616",
                null, null);
    }

}
