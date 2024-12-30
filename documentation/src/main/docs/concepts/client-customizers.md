# Client Customizers

Client customizers allow to customize the client instance created by the connector.
Only connectors which create their own client instance support customizers.

To define a client customizer, you need to provide a CDI bean that implements the `ClientCustomizer<T>` interface,
parameterized with the config type:

``` java
{{ insert('customizers/MyClientCustomizer.java') }}
```

Connectors which support client customizers will discover all beans and call the `customize` method,
with the _channel name_, the _channel configuration_ and the configuration that'll be used to create the client instance.
If the customizer returns `null` it'll be skipped.

If you have multiple customizers, customizers can override the `getPriority` method to define the order in which they are called.

Currently, the following core connectors support client customizers:

-   Kafka:    `ClientCustomizer<Map<String, Object>>`
-   RabbitMQ: `ClientCustomizer<RabbitMQOptions>`
-   AMQP 1.0: `ClientCustomizer<AmqpClientOptions>`
-   MQTT:     `ClientCustomizer<MqttClientSessionOptions>`
-   Pulsar:   `ClientCustomizer<ClientBuilder>`, `ClientCustomizer<ConsumerBuilder<?>>`, `ClientCustomizer<ProducerBuilder<?>>`
