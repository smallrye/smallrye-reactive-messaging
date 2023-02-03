# Advanced configuration

## Underlying thread pool

Lots of JMS operations are blocking and so not cannot be done on the
*caller* thread. For this reason, these blocking operations are executed
on a worker thread.

You can configure the thread pool providing these worker threads using
the following MicroProfile Config properties:

-   `smallrye.jms.threads.max-pool-size` - the max number of threads
    (Defaults to 10)

-   `smallrye.jms.threads.ttl` - the ttl of the created threads
    (Defaults to 60 seconds)

## Selecting the ConnectionFactory

The JMS Connector requires a `jakarta.jms.ConnectionFactory` to be exposed
as a CDI bean. The connector looks for a `jakarta.jms.ConnectionFactory`
and delegate the interaction with the JMS server to this factory.

In case you have several connection factories, you can use the
`@Identifier` qualifier on your factory to specify the name. Then, in
the channel configuration, configure the name as follows:

``` properties
# Configure the connector globally
mp.messaging.connector.smallrye-jms.connection-factory-name=my-factory-name
# Configure a specific incoming channel
mp.messaging.incoming.my-channel.connection-factory-name=my-factory-name
# Configure a specific outgoing channel
mp.messaging.outgoing.my-channel.connection-factory-name=my-factory-name
```
