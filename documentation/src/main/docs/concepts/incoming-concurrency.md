# Incoming Channel Concurrency

!!!warning "Experimental"
    Incoming channel `concurrency` config is an experimental feature.

The `concurrency` attribute for incoming channels provides a mechanism to enable concurrent non-blocking processing of incoming messages.
When applied to a channel, this attribute specifies the number of copies of that channel to be created and wired to the processing method,
allowing multiple messages to be processed concurrently.

For example, concurrency configuration for a Kafka incoming channel the configuration will look like:

```properties
mp.messaging.incoming.my-channel.connector=smallrye-kafka
mp.messaging.incoming.my-channel.topic=orders
mp.messaging.incoming.my-channel.value.deserializer=org.apache.kafka.common.serialization.DoubleDeserializer
mp.messaging.incoming.my-channel.concurrency=4
```

In this example, there will be 4 copies of the `my-channel` running concurrently, with distinctive internal channel names,
`my-channel$1`, `my-channel$2`, etc. but all registered with the name `my-channel` to the `ChannelRegistry`.

!!!info "Kafka connector `partitions`"
    This is essentially very similar to the Kafka connector `partitions` configuration, but addresses some its limitations.
    Using `partitions` config in Kafka connector, channels are merged into the downstream message processor
    (method annotated with `@Incoming` or an injected channel) which is therefore called sequentially.
    This prevents concurrently processing messages from multiple partitions.

    The `concurrency` mechanism effectively allows polling Kafka partitions from separate clients
    and concurrently processing records while preserving the in-partition order.

Copy channels inherit all configuration attributes of the main channel config.
Per-copy channel attributes can be configured separately using the `$` separated channel names: `mp.messaging.incoming.my-channel$1.attribute`.

For example, the following AMQP 1.0 channel defines 3 channels each with a different selector:

```properties
mp.messaging.incoming.data.connector=smallrye-amqp
mp.messaging.incoming.data.address=address
mp.messaging.incoming.data.durable=false
mp.messaging.incoming.data.concurrency=3
mp.messaging.incoming.data$1.selector=x='foo'
mp.messaging.incoming.data$2.selector=x='bar'
mp.messaging.incoming.data$3.selector=x='baz'
```

While the `concurrency` attribute is applicable to channels of any connector type,
the channel implementation may need to take this configuration into account and adjust the threading accordingly.
Connectors based on Vert.x event loop create a new event loop context per copy-channel to dispatch messages on distinct contexts.

!!!important "Non-blocking processing"
    Note that while this allows concurrent processing, messages are still dispatched on Vert.x event loop threads, and should not be blocked.

Otherwise, connectors treat copy channels as independent channels.
For example, health check reports are registered separately for each copy-channel.


