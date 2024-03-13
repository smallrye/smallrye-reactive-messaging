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

If you experience frequent disconnections from the broker, the AmqpClientOptions can also be used to set a heartbeat if you need to keep the AMQP connection permanently. 
Some brokers might terminate the AMQP connection after a certain idle timeout. 
You can provide a heartbeat value which will be used by the Vert.x proton client to advertise the idle timeout when opening transport to a remote peer.

```java
@Produces
@Identifier("my-named-options")
public AmqpClientOptions getNamedOptions() {
  // set a heartbeat of 30s (in milliseconds)
  return new AmqpClientOptions()
        .setHeartbeat(30000);
}
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
