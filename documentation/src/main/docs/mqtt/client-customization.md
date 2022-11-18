# Customizing the underlying MQTT client

You can customize the underlying MQTT Client configuration by
*producing* an instance of
`io.smallrye.reactive.messaging.mqtt.session.MqttClientSessionOptions`:

``` java
{{ insert('mqtt/customization/ClientProducers.java', 'named') }}
```

This instance is retrieved and used to configure the client used by the
connector. You need to indicate the name of the client using the
`client-options-name` attribute:

```properties
mp.messaging.incoming.prices.client-options-name=my-options
```
