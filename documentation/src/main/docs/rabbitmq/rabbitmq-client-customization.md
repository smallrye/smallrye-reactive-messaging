# Customizing the underlying RabbitMQ client

You can customize the underlying RabbitMQ Client configuration by
*producing* an instance of
[`RabbitMQOptions`](https://vertx.io/docs/apidocs/io/vertx/rabbitmq/RabbitMQOptions.html):

``` java
{{ insert('rabbitmq/customization/RabbitMQProducers.java', 'named') }}
```

This instance is retrieved and used to configure the client used by the
connector. You need to indicate the name of the client using the
`client-options-name` attribute:

    mp.messaging.incoming.prices.client-options-name=my-named-options

## Shared connections

By default, each channel opens its own connection to the RabbitMQ
broker. If your application has multiple channels connecting to the same
broker, you can configure them to share a single underlying connection
using the `shared-connection-name` attribute:

``` properties
mp.messaging.incoming.orders.connector=smallrye-rabbitmq
mp.messaging.incoming.orders.shared-connection-name=my-connection

mp.messaging.outgoing.confirmations.connector=smallrye-rabbitmq
mp.messaging.outgoing.confirmations.shared-connection-name=my-connection
```

In the above example, the `orders` incoming channel and the
`confirmations` outgoing channel share the same RabbitMQ connection.

All channels sharing a connection name **must** use identical connection
options (host, port, credentials, SSL settings, virtual host, etc.).
If two channels declare the same `shared-connection-name` but have
different connection options, the connector throws an
`IllegalStateException` at startup.

Shared connections are useful when your application has many channels
connecting to the same broker and you want to reduce the number of TCP
connections, or when the broker imposes connection limits.
