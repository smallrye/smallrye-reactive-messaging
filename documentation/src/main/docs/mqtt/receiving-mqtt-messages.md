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
instance that exposes the MQTT publish frame:

```java
ReceivingMqttMessageMetadata metadata = message
        .getMetadata(ReceivingMqttMessageMetadata.class)
        .orElseThrow();

String topic       = metadata.getTopic();
MqttQoS qos        = metadata.getQosLevel();
boolean retain     = metadata.isRetain();
boolean duplicate  = metadata.isDuplicate();
int messageId      = metadata.getMessageId();
```

When connected to a broker in MQTT 5.0 mode (`version=5`), the metadata
also exposes the MQTT 5.0 message properties:

| Accessor                       | Returns                                                |
|--------------------------------|--------------------------------------------------------|
| `getProperties()`              | The full `io.netty.handler.codec.mqtt.MqttProperties`  |
| `getUserProperties()`          | The User Properties (`List<StringPair>`)               |
| `getContentType()`             | The `Content-Type` property, or `null`                 |
| `getResponseTopic()`           | The `Response Topic` property, or `null`               |
| `getCorrelationData()`         | The `Correlation Data` bytes, or `null`                |
| `getMessageExpiryInterval()`   | The message expiry interval in seconds, or `null`      |
| `getPayloadFormatIndicator()`  | The payload format indicator (0=binary, 1=UTF-8)       |

For convenience the `MqttMessage` interface itself exposes
`getResponseTopic()` and `getCorrelationData()` directly, so that the
typical request/response handling code does not need to dig into the
metadata. See
[Sending messages to MQTT](sending-messages-to-mqtt.md#mqtt-5-requestresponse)
for an example of how to use them when replying.

## MQTT 5.0 subscription options

In addition to `qos`, three MQTT 5.0 subscription options can be set on
the inbound channel:

```properties
mp.messaging.incoming.prices.version=5
mp.messaging.incoming.prices.no-local=true
mp.messaging.incoming.prices.retain-as-published=true
mp.messaging.incoming.prices.retain-handling=2
```

- `no-local` (default `false`): when `true` the broker does not forward
  to this subscription the messages that were published by the same
  connection.
- `retain-as-published` (default `false`): preserves the `retain` flag
  on messages delivered to the subscriber instead of clearing it.
- `retain-handling` (default `0`): controls how retained messages are
  sent on subscribe — `0` send them on subscribe, `1` send them only if
  the subscription does not already exist, `2` never send retained
  messages on subscribe.

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
	
    
    
    
    
    
    
    
    
    
    
    
    
    
    
