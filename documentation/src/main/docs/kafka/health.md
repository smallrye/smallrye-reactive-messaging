# Health reporting

The Kafka connector reports the startup, readiness and liveness of each channel managed by the connector.

!!!note
    To disable health reporting, set the `health-enabled` attribute for the
    channel to `false`.

## Startup & Readiness

### Metrics-based strategy

By default, both inbound and outbound channels use underlying Kafka client metrics to check if at least one active connection exists with a broker.
This strategy is lightweight and does not require additional remote interactions with the broker.

### Client-based strategy

You can also enable another strategy by setting the `health-topic-verification-enabled` attribute to `true`.
With this second strategy, the health checks use a Kafka Admin Client to access the broker and retrieve the list of existing topics.
Retrieving this list can be a lengthy and expensive operation.
You can configure a timeout using the `health-topic-verification-timeout` attribute.
The default timeout is set to 2 seconds.
Note that if the timeout is reached, the health check fails.

!!!warning "Deprecated"
    `health-readiness-topic-verification` and `health-readiness-timeout` attributes are deprecated and replaced by `health-topic-verification-enabled` and `health-topic-verification-timeout`.

For **startup** checks both _inbound_ and _outbound_ side verify that the Kafka topic is created and its partitions are available in the broker.
If multiple topics are consumed using the `topics` attribute, the readiness check verifies that all the consumed topics are available.
If you use a pattern (using the `pattern` attribute), the readiness check verifies that at least one existing topic matches the pattern.

For **readiness** checks inbound channels verify that the underlying consumer is assigned at least a partition to consume.
On the outbound side (writing records to Kafka) verify that the broker is still accessible.

!!!note
    If `health-topic-verification-enabled` is enabled, both for startup and readiness checks use this strategy. They can be disabled explicitly using `health-topic-verification-startup-disabled` and `health-topic-verification-readiness-disabled` flags.


To summarize for startup and readiness health checks:

**Startup**

|               | Inbound                                                            | Outbound                                            |
|---------------|--------------------------------------------------------------------|-----------------------------------------------------|
| Metrics-based | `connection-count` metric > 0 or<br/> no subscribers to the stream | `connection-count` metric > 0                       |
| Client-based  | Subscribed topic(s) exist in the broker and are available          | Produced topic exist in the broker and is available |

**Readiness**

|               | Inbound                                                                          | Outbound                                       |
|---------------|----------------------------------------------------------------------------------|------------------------------------------------|
| Metrics-based | `connection-count` metric > 0 or<br/> no subscribers to the stream               | `connection-count` metric > 0                  |
| Client-based  | Consumer has at least one partition assignment<br/> no subscribers to the stream | Cluster is acessible using the Kafka admin API |


## Liveness

On the inbound side (receiving records from Kafka), the liveness check
verifies that:

-   no failures have been caught

-   the client is connected to the broker

On the outbound side (writing records to Kafka), the liveness check
verifies that:

-   no failures have been caught

Note that a message processing failures *nacks* the message which is
then handled by the failure-strategy. It the responsibility of the
failure-strategy to report the failure and influence the outcome of the
liveness checks. The `fail` failure strategy reports the failure and so
the liveness check will report the failure.
