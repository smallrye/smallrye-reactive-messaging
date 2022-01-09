# Customizing the underlying AMQP client

You can customize the underlying AMQP Client configuration by
*producing* an instance of
[`AmqpClientOptions`](https://vertx.io/docs/apidocs/io/vertx/amqp/AmqpClientOptions.html):

``` java
{{ insert('amqp/customization/ClientProducers.java', 'named') }}
```

This instance is retrieved and used to configure the client used by the
connector. You need to indicate the name of the client using the
`client-options-name` attribute:

```properties
mp.messaging.incoming.prices.client-options-name=my-named-options
```

## Client capabilities

Both incoming and outgoing AMQP channels can be configured with a list
of capabilities to declare during sender and receiver connections with
the AMQP broker. Note that supported capability names are broker
specific.

```properties
mp.messaging.incoming.sink.capabilities=temporary-topic
...
mp.messaging.outgoing.source.capabilities=shared
```
