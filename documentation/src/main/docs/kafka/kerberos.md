# Kerberos authentication

When using [Kerberos](https://en.wikipedia.org/wiki/Kerberos_(protocol))
authentication, you need to configure the connector with:

-   the security protocol set to `SASL_PLAINTEXT`

-   the SASL mechanism set to `GSSAPI`

-   the Jaas config configured with `Krb5LoginModule`

-   the Kerberos service name

The following snippet provides an example:

``` properties
kafka.bootstrap.servers=ip-192-168-0-207.us-east-2.compute.internal:9094
kafka.sasl.mechanism=GSSAPI
kafka.security.protocol=SASL_PLAINTEXT
kafka.sasl.jaas.config=com.sun.security.auth.module.Krb5LoginModule required doNotPrompt=true refreshKrb5Config=true useKeyTab=true storeKey=true keyTab="file:/opt/kafka/krb5/kafka-producer.keytab" principal="kafka-producer/ip-192-168-0-207.us-east-2.compute.internal@INTERNAL";
kafka.sasl.kerberos.service.name=kafka
```
