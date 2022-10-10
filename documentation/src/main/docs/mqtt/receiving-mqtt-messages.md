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

The MQTT connector does not provide inbound metadata.

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
	
    
    
    
    
    
    
    
    
    
    
    
    
    
    
