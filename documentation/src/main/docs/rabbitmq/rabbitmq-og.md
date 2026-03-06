# RabbitMQ OG Connector

The RabbitMQ OG Connector adds support for RabbitMQ to Reactive Messaging,
based on the AMQP 0-9-1 Protocol Specification.

Advanced Message Queuing Protocol 0-9-1 ([AMQP
0-9-1](https://www.rabbitmq.com/resources/specs/amqp0-9-1.pdf)) is an
open standard for passing business messages between applications or
organizations.

With this connector, your application can:

-   receive messages from a RabbitMQ queue
-   send messages to a RabbitMQ exchange

The RabbitMQ OG connector is based on the [RabbitMQ Java Client](https://www.rabbitmq.com/client-libraries/java-client),
the official RabbitMQ client library.

!!!note
    This connector is an alternative to the existing RabbitMQ connector
    (`smallrye-rabbitmq`), which is based on the Vert.x RabbitMQ client.
    The OG connector uses the original RabbitMQ Java client directly,
    providing improved reconnection handling and direct access to all
    RabbitMQ client features.

!!!important
    The **AMQP connector** supports the AMQP 1.0 protocol, which is very
    different from AMQP 0-9-1. You *can* use the AMQP connector with
    RabbitMQ provided that the latter has the [AMQP 1.0
    Plugin](https://github.com/rabbitmq/rabbitmq-amqp1.0/blob/v3.7.x/README.md)
    installed, albeit with reduced functionality.

## Using the RabbitMQ OG connector

To use the RabbitMQ OG Connector, add the following dependency to your
project:

``` xml
<dependency>
  <groupId>io.smallrye.reactive</groupId>
  <artifactId>smallrye-reactive-messaging-rabbitmq-og</artifactId>
  <version>{{ attributes['project-version'] }}</version>
</dependency>
```

The connector name is: `smallrye-rabbitmq-og`.

So, to indicate that a channel is managed by this connector you need:
```properties
# Inbound
mp.messaging.incoming.[channel-name].connector=smallrye-rabbitmq-og

# Outbound
mp.messaging.outgoing.[channel-name].connector=smallrye-rabbitmq-og
```
