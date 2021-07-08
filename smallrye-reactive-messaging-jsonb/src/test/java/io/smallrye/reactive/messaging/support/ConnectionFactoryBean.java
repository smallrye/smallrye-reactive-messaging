package io.smallrye.reactive.messaging.support;

import org.apache.activemq.artemis.jms.client.ActiveMQJMSConnectionFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.jms.ConnectionFactory;

@ApplicationScoped
public class ConnectionFactoryBean {

    @Produces
    ConnectionFactory factory() {
        return new ActiveMQJMSConnectionFactory(
                "tcp://localhost:61616",
                null, null);
    }

}
