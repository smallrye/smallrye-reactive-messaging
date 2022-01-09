# Health reporting

The AMQP connector reports the startup, liveness, and readiness of each
inbound (Receiving messages) and outbound (sending messages) channel
managed by the connector:

* Startup :: For both inbound and outbound, the startup probe reports *OK* when the
connection with the broker is established, and the AMQP senders and
receivers are opened (the links are attached to the broker).

* Liveness :: For both inbound and outbound, the liveness check verifies that the
connection is established. The check still returns *OK* if the
connection got cut, but we are attempting a reconnection.

* Readiness :: For the inbound, it checks that the connection is established and the
receiver is opened. Unlike the liveness check, this probe reports *KO*
until the connection is re-established. For the outbound, it checks that
the connection is established and the sender is opened. Unlike the
liveness check, this probe reports *KO* until the connection is
re-established.

!!!note
    To disable health reporting, set the `health-enabled` attribute for the
    channel to `false`.

Note that a message processing failures *nacks* the message, which is
then handled by the failure-strategy. It is the responsibility of the
`failure-strategy` to report the failure and influence the outcome of
the checks. The `fail` failure strategy reports the failure, and so the
check will report the fault.
