# AMQP 1.0 Connector

The AMQP Connector adds support for AMQP 1.0 to Reactive Messaging.

Advanced Message Queuing Protocol 1.0 (AMQP 1.0) is an open standard for
passing business messages between applications or organizations.

With this connector, your application can:

-   receive messages from an AMQP Broker or Router.
-   send `Message` to an AMQP *address*

The AMQP connector is based on the [Vert.x AMQP Client](https://vertx.io/docs/vertx-amqp-client/java/).

# Using the AMQP connector

To use the AMQP Connector, add the following dependency to your project:

``` xml
<dependency>
  <groupId>io.smallrye.reactive</groupId>
  <artifactId>smallrye-reactive-messaging-amqp</artifactId>
  <version>{{ attributes['project-version'] }}</version>
</dependency>
```

The connector name is: `smallrye-amqp`.

So, to indicate that a channel is managed by this connector you need:

```properties
# Inbound
mp.messaging.incoming.[channel-name].connector=smallrye-amqp

# Outbound
mp.messaging.outgoing.[channel-name].connector=smallrye-amqp
```

!!!important "RabbitMQ"
    To use RabbitMQ, refer to [Using RabbitMQ](#amqp-rabbitmq).

