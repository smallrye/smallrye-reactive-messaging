# PulsarClientService

For advanced use cases, SmallRye Reactive Messaging provides a bean of
type `PulsarClientService` that you can inject:

``` java
@Inject
PulsarClientService pulsarClients;
```

From there, you can obtain
`org.apache.pulsar.client.api.Consumer`,
`org.apache.pulsar.client.api.Producer` and `org.apache.pulsar.client.api.PulsarClient` by channel name.

!!!Important
    Note that the [`PulsarAdmin`](https://pulsar.apache.org/docs/3.0.x/admin-api-get-started/) is a separate library and the Pulsar connector doesn't provide it out-of-the-box.
    You'd need to explicitly add the dependency, i.e. `org.apache.pulsar:pulsar-client-admin-original` artifact.

