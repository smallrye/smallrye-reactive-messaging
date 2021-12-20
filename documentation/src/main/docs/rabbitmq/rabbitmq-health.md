# Health reporting

The RabbitMQ connector reports the readiness and liveness of each
channel managed by the connector.

On the inbound side (receiving messages from RabbitMQ), the check
verifies that the receiver is connected to the broker.

On the outbound side (sending records to RabbitMQ), the check verifies
that the sender is not disconnected from the broker; the sender *may*
still be in an initialized state (connection not yet attempted), but
this is regarded as live/ready.
