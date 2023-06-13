# Configuring Pulsar clients, consumers and producers

Pulsar clients, consumers and producers are very customizable to configure how a Pulsar client application behaves.

The Pulsar connector creates a Pulsar client and, a consumer or a producer per channel, each with sensible defaults to ease their configuration.
Although the creation is handled, all available configuration options remain configurable through Pulsar channels.

While idiomatic way of creating `PulsarClient`, `PulsarConsumer` or `PulsarProducer` are through builder APIs, in its essence
those APIs build each time a configuration object, to pass onto the implementation.
Those are {{ javadoc('org.apache.pulsar.client.impl.conf.ClientConfigurationData', False, 'org.apache.pulsar/pulsar-client-original') }},
{{ javadoc('org.apache.pulsar.client.impl.conf.ConsumerConfigurationData', False, 'org.apache.pulsar/pulsar-client-original') }},
{{ javadoc('org.apache.pulsar.client.impl.conf.ProducerConfigurationData', False, 'org.apache.pulsar/pulsar-client-original') }}.

Pulsar Connector allows receiving properties for those configuration objects directly.
For example, the broker authentication information for `PulsarClient` is received using `authPluginClassName` and `authParams` properties.
In order to configure the authentication for the incoming channel `data` :

```properties
mp.messaging.incoming.data.connector=smallrye-pulsar
mp.messaging.incoming.data.serviceUrl=pulsar://localhost:6650
mp.messaging.incoming.data.topic=topic
mp.messaging.incoming.data.subscriptionInitialPosition=Earliest
mp.messaging.incoming.data.schema=INT32
mp.messaging.incoming.data.authPluginClassName=org.apache.pulsar.client.impl.auth.AuthenticationBasic
mp.messaging.incoming.data.authParams={"userId":"superuser","password":"admin"}
```

Note that the Pulsar consumer property `subscriptionInitialPosition` is also configured with the `Earliest` value which represents with enum value `SubscriptionInitialPosition.Earliest`.

This approach covers most of the configuration cases.
However, non-serializable objects such as `CryptoKeyReader`, `ServiceUrlProvider` etc. cannot be configured this way.
The Pulsar Connector allows taking into account instances of Pulsar configuration data objects –
`ClientConfigurationData`, `ConsumerConfigurationData`, `ProducerConfigurationData`:

```java
{{ insert('pulsar/configuration/PulsarClientConfigProducers.java', 'consumer') }}
```

This instance is retrieved and used to configure the client used by the connector.
You need to indicate the name of the client using the `client-configuration`, `consumer-configuration` or `producer-configuration` attributes:

```properties
mp.messaging.incoming.prices.consumer-configuration=my-consumer-options
```

If no `[client|consumer|producer]-configuration` is configured, the connector will look for instances identified with the channel name:

```java
{{ insert('pulsar/configuration/PulsarClientConfigProducers.java', 'client') }}
```

You also can provide a `Map<String, Object>` containing configuration values by key:

```java
{{ insert('pulsar/configuration/PulsarClientConfigProducers.java', 'producer') }}
```


Different configuration sources are loaded in the following order of precedence, from the least important to the highest:

1. `Map<String, Object>` config map produced with default config identifier, `default-pulsar-client`, `default-pulsar-consumer`, `default-pulsar-producer`.
2. `Map<String, Object>` config map produced with identifier in the configuration or channel name
3. `[Client|Producer|Consuemr]ConfigurationData` object produced with identifier in the channel configuration or the channel name
4. Channel configuration properties named with `[Client|Producer|Consuemr]ConfigurationData` field names.

Following is the configuration reference for the `PulsarClient`.

Corresponding sections list the [Consumer Configuration Reference](../pulsar/receiving-pulsar-messages.md#configuration-reference) and
[Producer Configuration Reference](../pulsar/sending-messages-to-pulsar.md#configuration-reference).
Configuration properties not configurable in configuration files (non-serializable) is noted in the column `Config file`.

## `PulsarClient` Configuration Reference

{{ insert('../docs/pulsar/config/smallrye-pulsar-client.md') }}
