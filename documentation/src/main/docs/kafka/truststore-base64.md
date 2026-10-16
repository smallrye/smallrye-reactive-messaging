# Trust store from a base64 value

Kafka clients can only load a JKS or PKCS12 trust store from a file
(`ssl.truststore.location`). When the trust store is provided as a value,
for example through an environment variable or a secret, the connector can
take it directly with the `ssl.truststore.base64` attribute, so it does not
need to be written to disk first.

The value is the base64 encoding of a JKS or PKCS12 trust store, or of PEM
certificates. The connector reads the certificates and passes them to the
Kafka client as `ssl.truststore.type=PEM` and `ssl.truststore.certificates`.
For JKS and PKCS12, `ssl.truststore.password` is used to open the store if
it is set; it is not passed to the Kafka client.

``` properties
mp.messaging.connector.smallrye-kafka.bootstrap.servers=broker:9093
mp.messaging.connector.smallrye-kafka.security.protocol=SASL_SSL
mp.messaging.connector.smallrye-kafka.ssl.truststore.base64=${KAFKA_TRUSTSTORE_BASE64}
```

Like any other attribute, it can also be set per channel, for example
`mp.messaging.incoming.prices.ssl.truststore.base64`, or in the
[default Kafka configuration](default-configuration.md).

`ssl.truststore.base64` cannot be combined with `ssl.truststore.location`
or `ssl.truststore.certificates`: the channel fails to start if more than one
is set, or if the value cannot be read or contains no certificates.
