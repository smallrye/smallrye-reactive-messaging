# Using RabbitMQ

This connector is for AMQP 1.0. RabbitMQ implements AMQP 0.9.1. RabbitMQ
does not provide AMQP 1.0 by default, but there is a plugin for it. To
use RabbitMQ with this connector, enable and configure the [AMQP 1.0
plugin](https://github.com/rabbitmq/rabbitmq-amqp1.0/blob/v3.8.x/README.md).

Despite the plugin, a few features won’t work with RabbitMQ. Thus, we
recommend the following configurations.

To receive messages from RabbitMQ:

-   Set `durable` to `false`

``` properties
mp.messaging.incoming.prices.connector=smallrye-amqp
mp.messaging.incoming.prices.durable=false
```

To send messages to RabbitMQ:

-   set the destination `address` (anonymous sender are not supported)

``` properties
mp.messaging.outgoing.generated-price.connector=smallrye-amqp
mp.messaging.outgoing.generated-price.address=prices
```

It’s not possible to change the destination dynamically (using message
metadata) when using RabbitMQ. The connector automatically detects that
the broker does not support anonymous sender (See
<http://docs.oasis-open.org/amqp/anonterm/v1.0/anonterm-v1.0.html>).

Alternatively, you can use the [RabbitMQ connector](../rabbitmq/rabbitmq.md).
