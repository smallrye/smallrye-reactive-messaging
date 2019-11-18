package io.smallrye.reactive.messaging.jms.support;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.jms.ConnectionFactory;

import org.apache.activemq.artemis.jms.client.ActiveMQJMSConnectionFactory;

@ApplicationScoped
public class ConnectionFactoryBean {

    @Produces
    ConnectionFactory factory() {
        ActiveMQJMSConnectionFactory factory = new ActiveMQJMSConnectionFactory(
                "tcp://localhost:61616",
                null, null);
        return factory;
    }

}
