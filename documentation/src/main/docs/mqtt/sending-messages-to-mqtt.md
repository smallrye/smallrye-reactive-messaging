# Sending messages to MQTT

The MQTT Connector can write Reactive Messaging `Messages` as MQTT
Message.

## Example

Let’s imagine you have a MQTT server/broker running, and accessible
using the `mqtt:1883` address (by default it would use
`localhost:1883`). Configure your application to write the messages from
the `prices` channel into a MQTT Messages as follows:

```properties
mp.messaging.outgoing.prices.type=smallrye-mqtt
mp.messaging.outgoing.prices.host=mqtt
mp.messaging.outgoing.prices.port=1883
```

1.  Sets the connector for the `prices` channel
2.  Configure the broker/server host name.
3.  Configure the broker/server port. 1883 is the default.

!!!note
    You don’t need to set the MQTT topic. By default, it uses the channel
    name (`prices`). You can configure the `topic` attribute to override it.
    NOTE: It is generally recommended to set the `client-id`. By default,
    the connector is generating a unique `client-id`.


Then, your application must send `Message<Double>` to the `prices`
channel. It can use `double` payloads as in the following snippet:

``` java
{{ insert('mqtt/outbound/MqttPriceProducer.java') }}
```

Or, you can send `Message<Double>`:

``` java
{{ insert('mqtt/outbound/MqttPriceMessageProducer.java') }}
```

## Serialization

The `Message` sent to MQTT can have various payload types:

-   [`JsonObject`](https://vertx.io/docs/apidocs/io/vertx/core/json/JsonObject.html):
    JSON string encoded as `byte[]`

-   [`JsonArray`](https://vertx.io/docs/apidocs/io/vertx/core/json/JsonArray.html):
    JSON string encoded as `byte[]`

-   `java.lang.String` and Java primitive types: `toString` encoded as
    `byte[]`

-   `byte[]`

-   complex objects: The objects are encoded to JSON and passed as
    `byte[]`

## Outbound Metadata

The MQTT connector does not provide outbound metadata.

## Acknowledgement

MQTT acknowledgement depends on the QoS level. The message is
acknowledged when the broker indicated the successful reception of the
message (or immediately if the level of QoS does not support
acknowledgment).

If a MQTT message cannot be sent to the broker, the message is `nacked`.

## Configuration Reference

{{ insert('../../../target/connectors/smallrye-mqtt-outgoing.md') }}


The MQTT connector is based on the [Vert.x MQTT
client](https://vertx.io/docs/vertx-mqtt/java/#_vert_x_mqtt_client). So
you can pass any attribute supported by this client.

!!!important
    A single instance of `MqttClient` and a single connection is used for
    each `host` / `port` / `server-name` / `client-id`. This client is
    reused for both the inbound and outbound connectors.
