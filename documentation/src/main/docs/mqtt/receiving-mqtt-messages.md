# Receiving messages from MQTT

The MQTT Connector connects to a MQTT broker or router, and forward the
messages to the Reactive Messaging application. It maps each of them
into Reactive Messaging `Messages`.

## Example

Let’s imagine you have a MQTT server/broker running, and accessible
using the `mqtt:1883` address (by default it would use
`localhost:1883`). Configure your application to receive MQTT messages
on the `prices` channel as follows:

```properties
mp.messaging.incoming.prices.connector=smallrye-mqtt # <1>
mp.messaging.incoming.prices.host=mqtt # <2>
mp.messaging.incoming.prices.port=1883 # <3>
```
1.  Sets the connector for the `prices` channel
2.  Configure the broker/server host name.
3.  Configure the broker/server port. 1883 is the default.

!!!note
    You don’t need to set the MQTT topic. By default, it uses the channel
    name (`prices`). You can configure the `topic` attribute to override it.

!!!note
    It is generally recommended to set the `client-id`. By default, the connector is generating a unique `client-id`.

!!!important
    Message coming from MQTT have a `byte[]` payload.

Then, your application receives `Message<byte[]>`. You can consume the
payload directly:

``` java
{{ insert('mqtt/inbound/MqttPriceConsumer.java') }}
```

Or, you can retrieve the `Message<byte[]>`:

``` java
{{ insert('mqtt/inbound/MqttPriceMessageConsumer.java') }}
```

The inbound topic can use the [MQTT
wildcards](https://mosquitto.org/man/mqtt-7.html) (`+` and `#`).

## Deserialization

The MQTT Connector does not handle the deserialization and creates a
`Message<byte[]>`.

## Inbound Metadata

Each incoming `Message` carries a
`io.smallrye.reactive.messaging.mqtt.ReceivingMqttMessageMetadata`
instance describing the MQTT publish frame it comes from:

```java
ReceivingMqttMessageMetadata metadata = message
        .getMetadata(ReceivingMqttMessageMetadata.class)
        .orElseThrow();

String topic      = metadata.getTopic();
MqttQoS qos       = metadata.getQosLevel();
boolean retain    = metadata.isRetain();
boolean duplicate = metadata.isDuplicate();
int messageId     = metadata.getMessageId();
```

When the channel is configured with `mqtt-version=5`, the metadata also
exposes the MQTT 5.0 message properties:

| Accessor                      | Returns                                               |
|-------------------------------|-------------------------------------------------------|
| `getProperties()`             | The full `io.netty.handler.codec.mqtt.MqttProperties` |
| `getUserProperties()`         | The user properties, as a `Map<String, String>`       |
| `getContentType()`            | The `Content-Type` property, or `null`                |
| `getResponseTopic()`          | The `Response Topic` property, or `null`              |
| `getCorrelationData()`        | The `Correlation Data` bytes, or `null`               |
| `getMessageExpiryInterval()`  | The message expiry interval in seconds, or `null`     |
| `getPayloadFormatIndicator()` | The payload format indicator (0=binary, 1=UTF-8)      |
| `getSubscriptionIdentifier()` | The subscription identifier, or `null`                |

The `MqttMessage` interface exposes `getResponseTopic()` and
`getCorrelationData()` directly, so that replying to a request does not
require digging into the metadata. See
[Sending messages to MQTT](sending-messages-to-mqtt.md) for an example.

## MQTT 5.0 subscription options

Besides `qos`, three MQTT 5.0 subscription options can be set on an
inbound channel:

```properties
mp.messaging.incoming.prices.mqtt-version=5
mp.messaging.incoming.prices.no-local=true
mp.messaging.incoming.prices.retain-as-published=true
mp.messaging.incoming.prices.retain-handling=1
mp.messaging.incoming.prices.subscription-identifier=42
```

`no-local` asks the broker not to send back the messages published by
this same client, `retain-as-published` keeps the retain flag as set by
the publisher, and `retain-handling` decides whether the retained
messages are sent on subscription (0), only if the subscription did not
already exist (1), or never (2).

## Failure Management

If a message produced from a MQTT message is *nacked*, a failure
strategy is applied. The MQTT connector supports 3 strategies:

-   `fail` - fail the application, no more MQTT messages will be
    processed. (default) The offset of the record that has not been
    processed correctly is not committed.

-   `ignore` - the failure is logged, but the processing continue.

## Configuration Reference

{{ insert('../../../target/connectors/smallrye-mqtt-incoming.md') }}

The MQTT connector is based on the [Vert.x MQTT
client](https://vertx.io/docs/vertx-mqtt/java/#_vert_x_mqtt_client). So
you can pass any attribute supported by this client.

!!!important
    A single instance of `MqttClient` and a single connection is used for
    each `host` / `port` / `server-name` / `client-id`. This client is
    reused for both the inbound and outbound connectors.
    
!!!important
	Using `auto-clean-session=false` the MQTT Connector send Subscribe requests
	to the broken only if a Persistent Session is not present (like on the first 
	connection). This means that if a Session is already present (maybe for a 
	previous run) and you add a new incoming channel, this will not be subscribed.
	Beware to check always the subscription present on Broker when use 
	`auto-clean-session=false`.
	
## Manage buffer overflow / message spikes from the MQTT broker

When the MQTT client is slow to process messages, or when there is a message spike from the broker, a backpressure mechanism temporarily pauses reading from the broker's TCP stream. This causes unread data to accumulate first in the OS TCP receive buffer, and then in the broker's send queue—naturally propagating backpressure upstream.

The mechanism is driven by the fill level of the message buffer(s). When any channel's buffer exceeds the pause threshold (default: 70% of buffer-size), the TCP input stream is paused. Reading resumes only when all channel buffers for that client drop below the resume threshold (default: 40% of buffer-size).

These thresholds must be chosen carefully. They are evaluated against the application-level message buffer, sized by the buffer-size parameter (default: 128 messages). However, when the pause is triggered, it takes effect after the OS TCP receive buffer (receive-buffer-size, defaulting to the OS value), but before the Netty pipeline's receive buffer (RecvByteBufAllocator, configured via recv-bytebuf-allocator-size; if set to -1, it defaults to the receive-buffer-size, otherwise typically 2 KB).

This means that, depending on the average MQTT message size, a significant number of messages may already be queued in the Netty receive buffer. After the read is paused, these bytes are still processed and decoded into MQTT messages, which are then pushed into the application-level message buffer. For example, a pause threshold of 70% leaves 30% of the buffer capacity (in terms of message count), but the already buffered bytes may decode into enough MQTT messages to fully consume—or even overflow—that remaining 30%.

{{ image('../../images/pauseresume.png', 'Pause/Resume Input Stream') }}

If the application-level message buffer becomes full, the client will not receive additional messages, and the following exception appear in the logs:

```
io.smallrye.mutiny.subscription.BackPressureFailure: The overflow buffer is full, which is due to the upstream sending too many items w.r.t. the downstream capacity and/or the downstream not consuming items fast enough
```

    
    
    
    
    
    
    
    
    
    
    
    
    
