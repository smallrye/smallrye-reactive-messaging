# Retrieving Kafka default configuration

If your application/runtime exposes as a CDI *bean* of type
`Map<String, Object` with the identifier `default-kafka-broker`, this
configuration is used to establish the connection with the Kafka broker.

For example, you can imagine exposing this map as follows:

``` java
@Produces
@ApplicationScoped
@Identifier("default-kafka-broker")
public Map<String, Object> createKafkaRuntimeConfig() {
    Map<String, Object> properties = new HashMap<>();

    StreamSupport
        .stream(config.getPropertyNames().spliterator(), false)
        .map(String::toLowerCase)
        .filter(name -> name.startsWith("kafka"))
        .distinct()
        .sorted()
        .forEach(name -> {
            final String key = name.substring("kafka".length() + 1).toLowerCase().replaceAll("[^a-z0-9.]", ".");
            final String value = config.getOptionalValue(name, String.class).orElse("");
            properties.put(key, value);
        });

    return properties;
}
```

This previous example would extract all the configuration keys from
MicroProfile Config starting with `kafka`.

!!!note "Quarkus"
    Starting with Quarkus 1.5, a map corresponding to the previous example
    is automatically provided.

In addition to this default configuration, you can configure the name of
the `Map` producer using the `kafka-configuration` attribute:

``` properties
mp.messaging.incoming.my-channel.connector=smallrye-kafka
mp.messaging.incoming.my-channel.kafka-configuration=my-configuration
```

In this case, the connector looks for the `Map` associated with the
`my-configuration` name. If `kafka-configuration` is not set, an
optional lookup for a `Map` exposed with the channel name (`my-channel`
in the previous example) is done.

!!!important
    If `kafka-configuration` is set and no `Map` can be found, the
    deployment fails.

Attribute values are resolved as follows:

1.  if the attribute is set directly on the channel configuration
    (`mp.messaging.incoming.my-channel.attribute=value`), this value is
    used

2.  if the attribute is not set on the channel, the connector looks for
    a `Map` with the channel name or the configured
    `kafka-configuration` (if set) and the value is retrieved from that
    `Map`

3.  If the resolved `Map` does not contain the value the default `Map`
    is used (exposed with the `default-kafka-broker` name)
