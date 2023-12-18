# Health reporting

The RabbitMQ connector reports the readiness and liveness of each
channel managed by the connector.

On the inbound side (receiving messages from RabbitMQ), the check
verifies that the receiver is connected to the broker.

On the outbound side (sending records to RabbitMQ), the check verifies
that the sender is not disconnected from the broker; the sender *may*
still be in an initialized state (connection not yet attempted), but
this is regarded as live/ready.

You can disable health reporting by setting the `health-enabled` attribute of the channel to `false`.
It disables both liveness and readiness.
You can disable readiness reporting by setting the `health-readiness-enabled` attribute of the channel to `false`.

## @Channel and lazy subscription

When you inject a channel using `@Channel` annotation, you are responsible for subscribing to the channel.
Until the subscription happens, the channel is not connected to the broker and thus cannot receive messages.
The default health check will fail in this case.

To handle this use case, you need to configure the `health-lazy-subscription` attribute of the channel to `true`.
It configures the health check to not fail if there are no subscription yet.

