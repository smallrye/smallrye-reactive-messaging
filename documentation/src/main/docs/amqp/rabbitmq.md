# Using RabbitMQ

RabbitMQ 4.0+ provides native AMQP 1.0 support, making it fully compatible with this connector.
For older versions (3.x), RabbitMQ implements AMQP 0.9.1 and requires the
[AMQP 1.0 plugin](https://github.com/rabbitmq/rabbitmq-amqp1.0/blob/v3.8.x/README.md) to be enabled.

## Address format

RabbitMQ 4.0+ uses the v2 address format with `/queues/` and `/exchanges/` prefixes.
When using this connector with RabbitMQ, prefix your addresses accordingly:

``` properties
mp.messaging.incoming.prices.connector=smallrye-amqp
mp.messaging.incoming.prices.address=/queues/prices

mp.messaging.outgoing.generated-price.connector=smallrye-amqp
mp.messaging.outgoing.generated-price.address=/queues/prices
mp.messaging.outgoing.generated-price.use-anonymous-sender=false
```

## Recommendations

RabbitMQ support for AMQP 1.0 may differ from other AMQP 1.0 supporting brokers.
We recommend the following configurations.

To receive messages from RabbitMQ:

-   Set `durable` to `false`

``` properties
mp.messaging.incoming.prices.connector=smallrye-amqp
mp.messaging.incoming.prices.durable=false
```

To send messages to RabbitMQ:

-   set the destination `address` (anonymous senders are not supported)
-   set `use-anonymous-sender` to `false`

``` properties
mp.messaging.outgoing.generated-price.connector=smallrye-amqp
mp.messaging.outgoing.generated-price.address=prices
mp.messaging.outgoing.generated-price.use-anonymous-sender=false
```

It’s not possible to change the destination dynamically (using message
metadata) when using RabbitMQ. The connector automatically detects that
the broker does not support anonymous sender (See
<http://docs.oasis-open.org/amqp/anonterm/v1.0/anonterm-v1.0.html>).

## Request/Reply with RabbitMQ

This connector supports the request-reply pattern with RabbitMQ, including
RabbitMQ’s [Direct Reply-To](https://www.rabbitmq.com/docs/direct-reply-to) mechanism.
See [AMQP Request/Reply](request-reply.md#using-with-rabbitmq) for details.

Alternatively, you can use the [RabbitMQ connector](../rabbitmq/rabbitmq.md).
