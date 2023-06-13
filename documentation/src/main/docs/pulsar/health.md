# Health reporting

The Pulsar connector reports the startup, readiness and liveness of each channel managed by the connector:

* **Startup & Readiness** For both inbound and outbound, probes report *OK* when the
  connection with the broker is established.

* **Liveness** For both inbound and outbound, the liveness check verifies that the
  connection is established **AND** that no failures have been caught.

!!!note
    To disable health reporting, set the `health-enabled` attribute for the
    channel to `false`.

Note that a message processing failures *nacks* the message which is
then handled by the failure-strategy. It is the responsibility of the
failure-strategy to report the failure and influence the outcome of the
liveness checks. The `fail` failure strategy reports the failure and so
the liveness check will report the failure.
