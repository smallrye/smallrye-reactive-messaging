# Health reporting

The Kafka connector reports the readiness and liveness of each channel
managed by the connector.

!!!note
    To disable health reporting, set the `health-enabled` attribute for the
    channel to `false`.

## Readiness

On the inbound side, two strategies are available to check the readiness
of the application. The default strategy verifies that we have at least
one active connection with the broker. This strategy is lightweight.

You can also enable another strategy by setting the
`health-readiness-topic-verification` attribute to `true`. In this case,
the check verifies that:

-   the broker is available

-   the Kafka topic is created (available in the broker).

-   no failures have been caught

With this second strategy, if you consume multiple topics using the
`topics` attribute, the readiness check verifies that all the consumed
topics are available. If you use a pattern (using the `pattern`
attribute), the readiness check verifies that at least one existing
topic matches the pattern.

On the outbound side (writing records to Kafka), two strategies are also
offered. The default strategy just verifies that the producer has at
least one active connection with the broker.

You can also enable another strategy by setting the
`health-readiness-topic-verification` attribute to `true`. In this case,
teh check verifies that

-   the broker is available

-   the Kafka topic is created (available in the broker).

With this second strategy, the readiness check uses a Kafka Admin Client
to retrieve the existing topics. Retrieving the topics can be a lengthy
operation. You can configure a timeout using the
`health-readiness-timeout` attribute. The default timeout is set to 2
seconds.

Also, you can disable the readiness checks altogether by setting
`health-readiness-enabled` to `false`.

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
