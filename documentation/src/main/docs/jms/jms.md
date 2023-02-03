# The JMS connector

The JMS connector adds support for [Jakarta
Messaging](https://en.wikipedia.org/wiki/Jakarta_Messaging) to Reactive
Messaging. It is designed to integrate with *JakartaEE* applications
that are sending or receiving Jakarta Messaging Messages.

Jakarta Messaging is a Java Message Oriented Middleware API for sending
messages between two or more clients. It is a programming model to
handle the producer-consumer messaging problem. It is a messaging
standard that allows application components based on Jakarta EE to
create, send, receive, and read messages. It allows the communication
between different components of a distributed application to be loosely
coupled, reliable, and asynchronous.

# Using the JMS connector

To you the JMS Connector, add the following dependency to your project:

``` xml
<dependency>
  <groupId>io.smallrye.reactive</groupId>
  <artifactId>smallrye-reactive-messaging-jms</artifactId>
  <version>{{ attributes['project-version'] }}</version>
</dependency>
```

The connector name is: `smallrye-jms`.

So, to indicate that a channel is managed by this connector you need:

```properties
# Inbound
mp.messaging.incoming.[channel-name].connector=smallrye-jms

# Outbound
mp.messaging.outgoing.[channel-name].connector=smallrye-jms
```

The JMS Connector requires a `jakarta.jms.ConnectionFactory` to be exposed
(as CDI bean). The connector looks for a `jakarta.jms.ConnectionFactory`
and delegate the interaction with the JMS server to this factory. In
other words, it creates the JMS connection and context using this
factory.

So, in order to use this connector you would need to expose a
`jakarta.jms.ConnectionFactory`:

``` java
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
```

The factory class may depend on your JMS connector/server.

