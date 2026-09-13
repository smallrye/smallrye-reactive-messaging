# Customizing the underlying RabbitMQ client

You can customize the underlying RabbitMQ Client configuration by
*producing* an instance of
[`ConnectionFactory`](https://rabbitmq.github.io/rabbitmq-java-client/api/current/com/rabbitmq/client/ConnectionFactory.html):

``` java
{{ insert('rabbitmq/og/customization/RabbitMQProducers.java', 'named') }}
```

This instance is retrieved and used to configure the client used by the
connector. You need to indicate the name of the client using the
`client-options-name` attribute:

    mp.messaging.incoming.prices.client-options-name=my-named-options

## Credentials Provider

The OG connector supports RabbitMQ's
[`CredentialsProvider`](https://rabbitmq.github.io/rabbitmq-java-client/api/current/com/rabbitmq/client/impl/CredentialsProvider.html)
interface for dynamic credential management. This is useful when
credentials are rotated or fetched from an external secrets manager.

To use a credentials provider, expose a CDI bean implementing
`com.rabbitmq.client.impl.CredentialsProvider` with an `@Identifier`
qualifier, and reference it via the `credentials-provider-name` attribute:

```properties
mp.messaging.incoming.prices.credentials-provider-name=my-credentials-provider
```

## Cluster mode

To connect to a RabbitMQ cluster, use the `addresses` attribute to
specify multiple broker addresses. When set, this overrides the `host`
and `port` attributes:

```properties
rabbitmq-addresses=host1:5672,host2:5672,host3:5672
```

## NIO Sockets

The connector supports using NIO sockets for the RabbitMQ connection.
Enable NIO mode with:

```properties
rabbitmq-use-nio=true
```
