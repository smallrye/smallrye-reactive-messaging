# Graceful Shutdown

When a reactive messaging application shuts down, messages may still be in-flight — delivered to a processing method but not yet acknowledged.
Cancelling the reactive streams and closing connectors immediately can lead to message loss or duplicate processing on restart.

Graceful shutdown, enabled by default for every connector channel, addresses this by:

1. **Pausing** channels to stop receiving new messages
2. **Draining** in-flight messages, waiting for them to be acknowledged or negatively acknowledged
3. **Shutting down** connectors per-channel in a controlled sequence

## Configuring Graceful Shutdown

Graceful shutdown is enabled by default for every connector channel, incoming and outgoing.
A connector can declare another default for its incoming channels; the framework uses it when the property is not set.
All the connectors provided by SmallRye Reactive Messaging enable it.

To disable it on a channel, set the `graceful-shutdown` property to `false`:

```properties
mp.messaging.incoming.data.graceful-shutdown=false
mp.messaging.outgoing.out.graceful-shutdown=false
```

The value is resolved in this order: the channel property, the connector-level property (`mp.messaging.connector.<connector>.graceful-shutdown`), then the default.
To disable graceful shutdown for every Kafka channel:

```properties
mp.messaging.connector.smallrye-kafka.graceful-shutdown=false
```

To disable it for every channel that does not enable it explicitly, incoming and outgoing, whatever the connector:

```properties
smallrye.messaging.graceful-shutdown.default.enabled=false
```

This global property only changes the default: a channel or connector-level `graceful-shutdown` property still applies.
For example, to keep graceful shutdown off except for one channel:

```properties
smallrye.messaging.graceful-shutdown.default.enabled=false
mp.messaging.incoming.orders.graceful-shutdown=true
```

!!! note "Implementing a connector"
    See [Graceful shutdown in connectors](contributing-connectors.md#graceful-shutdown) for what a connector needs to do.

### Drain Timeout

The drain timeout controls how long the framework waits for in-flight messages to complete.
If the timeout is reached, the shutdown proceeds regardless.
The default is 10 seconds (10000 milliseconds).

```properties
mp.messaging.incoming.data.graceful-shutdown.drain-timeout=15000
```

As `graceful-shutdown`, it can be set for every channel of a connector:

```properties
mp.messaging.connector.smallrye-kafka.graceful-shutdown.drain-timeout=5000
```

To change it for every channel that sets it neither on the channel nor on its connector,
use the `smallrye.messaging.graceful-shutdown.default.drain-timeout` global property:

```properties
smallrye.messaging.graceful-shutdown.default.drain-timeout=2000
```

## How It Works

### Incoming Channels

When the application shuts down with graceful shutdown enabled on an incoming channel:

1. The channel is **paused** — no new messages are requested from the connector
2. In-flight messages are **drained** — the framework waits for all delivered messages to be acknowledged or negatively acknowledged
3. The connector's **pre-shutdown** hook is called — allowing the connector to flush commits or finalize handlers while the transport client is still alive
4. The connector's **shutdown** hook is called — closing the transport client and releasing resources

All incoming channels are shut down before outgoing channels, ensuring that consumers stop before producers.

``` java
{{ insert('shutdown/GracefulShutdownExample.java', 'consumer') }}
```

### Outgoing Channels

On an outgoing channel, the drain waits until the connector has acknowledged every message it received,
which usually means that the broker accepted it (see [Limitations](#limitations)).
How new messages are stopped depends on what feeds the channel.

When the channel is fed by an `Emitter`, the emitter is **completed** during shutdown.
This flushes any buffered messages downstream to the connector; the drain then waits until the completion has gone
through the channel and every message is acknowledged.

``` java
{{ insert('shutdown/EmitterShutdownExample.java', 'emitter') }}
```

When the channel is fed by a method (`@Outgoing`) or a stream, the channel is **paused**: no new messages are requested,
and the drain waits for the messages already sent to the connector to be acknowledged.

The connector's pre-shutdown and shutdown hooks are called after the drain.

### Shutdown Sequence

The shutdown is split into three phases, orchestrated by the `ChannelLifecycleManager`:

```
Phase 1: Prepare close incoming — prepareCloseIncoming()
  │  (application still functional, accepting requests)
  │
  └─ For each incoming channel (concurrently, up to 10):
       1. Pause and drain in-flight messages
       2. Connector prepare-close (e.g., flush commits),
          once per indexed channel for a channel configured with concurrency

Phase 2: Prepare close outgoing — prepareCloseOutgoing()
  │  (new requests rejected, emitters no longer receiving)
  │
  └─ For each outgoing channel (concurrently, up to 10):
       1. Complete the emitters (or pause the channel), and wait until
          the messages sent are acknowledged
       2. Connector prepare-close (e.g., flush pending sends)

Phase 3: Close — close()
  │
  ├─ For each incoming channel: close transport client
  ├─ For each outgoing channel: close transport client
  └─ For each connector (once):
       Terminate connector-wide resources (e.g., shared clients)
```

Incoming channels are drained first, while the application is still functional.
In-flight messages complete their full processing chain — including outgoing sends — via ack chaining.
Outgoing channels (emitters) are drained later, after new external requests
have been rejected, ensuring no new messages arrive while draining.

Each phase handles 10 channels at a time, and has a timeout:
the longest drain timeout of the channels, once for every 10 channels, plus 20 seconds for the connector calls.
With the default drain timeout, that is 30 seconds for up to 10 channels, 40 seconds for up to 20, and so on.
The timeout is an upper bound: a phase ends as soon as its channels are done.
It can be set with the `smallrye.messaging.graceful-shutdown.phase-timeout` property (milliseconds).
If a channel fails within the timeout, the failure is logged and the shutdown continues with the remaining channels.
Each phase runs at most once: later calls return immediately.

!!!note
    Drain guarantees apply to channels using `POST_PROCESSING` acknowledgment (the default).
    Channels with `PRE_PROCESSING` acknowledge messages before processing completes,
    so drain cannot guarantee that in-flight processing has finished.

### CDI and External Orchestrators

In a plain CDI environment, the `ChannelLifecycleManager` automatically runs all three phases
sequentially via a `@BeforeDestroyed(ApplicationScoped.class)` observer.

When an external shutdown orchestrator is available (e.g., Quarkus's service graph),
it can call individual phases at the appropriate time:

- `prepareCloseIncoming()` — while the app is still alive, drain incoming channels
- `prepareCloseOutgoing()` — after HTTP closes, drain outgoing channels
- `close()` — during ordered service teardown, close clients

Each phase runs at most once: the CDI observer skips the phases the orchestrator has already started,
including a phase that is still running.

## Pausable Channels and Drain

Graceful shutdown is built on top of [Pausable Channels](pausable-channels.md).
When `graceful-shutdown` is enabled, the channel is automatically configured as pausable,
unless `pausable` is explicitly set to `false`.

The `PausableChannel` interface provides `pauseAndDrain()` for programmatic use outside of shutdown:

``` java
{{ insert('shutdown/PauseAndDrainExample.java', 'pause-drain') }}
```

This is useful for operations that require no in-flight processing, such as seeking a Kafka consumer to a new position.

## Make Sure Messages Are Acknowledged

The drain waits for in-flight messages to be acknowledged or negatively acknowledged.
A message that is never acknowledged keeps the drain waiting until the drain timeout:
the shutdown takes that long, and warning `SRMSG00246` gives the channel and the number of messages still in flight.
Common causes:

- **A method receiving a `Message` with manual acknowledgement does not acknowledge it.**
  Acknowledge the message (`message.ack()`), or return a message created from it, which keeps its acknowledgement.
  See the [signature list](signatures.md) for the default acknowledgement strategy of each signature.
- **A method creates a new message from the payload and metadata of the incoming one**, such as
  `Message.of(payload, input.getMetadata())`: the new message does not acknowledge the incoming one.
  Use `input.withPayload(payload)` instead.
- **A custom acknowledgement replaces the incoming one**, such as `input.withPayload(payload).withAck(...)`:
  the custom acknowledgement must call `input.ack()`.
- **A connector does not acknowledge the messages it sends**, which keeps outgoing drains waiting.

If messages of a channel are not meant to be acknowledged, use `@Acknowledgment(Acknowledgment.Strategy.NONE)`:
channels consumed this way are not tracked (see [Limitations](#limitations)).

!!! tip "Tests"
    Graceful shutdown applies when a test stops the application, so a test that leaves messages unacknowledged
    waits for the drain timeout.
    Acknowledge the messages, or set `graceful-shutdown=false` (or a short `graceful-shutdown.drain-timeout`)
    on the channels of such tests.
    Let the container shutdown terminate the connectors: terminating a connector before the container shutdown
    makes the drain wait for messages that can no longer be acknowledged.

## Limitations

What the drain waits for:

- **Acknowledgment strategy:** in-flight messages are tracked through their acknowledgment.
  With `PRE_PROCESSING` acknowledgment, messages leave the in-flight count before they are processed,
  so the drain does not wait for their processing.
  An incoming channel consumed with `NONE` acknowledgment is not tracked at all (log `SRMSG00248`):
  the channel is still paused on shutdown, but the drain does not wait.
- **Messages never acknowledged:** the drain waits until the drain timeout, logs a warning (`SRMSG00246`)
  with the number of messages still in flight, and the shutdown continues.
- **Broadcast channels:** a message is counted once, so the drain completes when the first subscriber acknowledges it,
  even if another subscriber is still processing it.
- **Only connector channels are paused and drained.** Channels between methods are drained indirectly,
  when the acknowledgment of the processed messages reaches the incoming connector message.

What happens to the remaining messages:

- **Messages received but not yet delivered** when an incoming channel is paused are neither processed nor acknowledged.
  Brokers that redeliver unacknowledged messages (Kafka, AMQP, Pulsar, RabbitMQ with manual acknowledgment)
  deliver them again after a restart.
  With connectors that acknowledge on receipt, such as RabbitMQ with `auto-acknowledgement=true`, or MQTT,
  whose client acknowledges QoS 1 and QoS 2 messages when it receives them, they are lost.
- **Kafka:** a paused channel keeps its consumer polling, so it stays in the consumer group: the records fetched
  fill the queue of the channel, up to `max-queue-size-factor` times `max.poll.records`, then the partitions are paused.
  These records are not delivered nor committed, so they are consumed again after a restart.
  The offsets of the messages processed are committed at the end of the drain of the channel.
  A message still in flight after the drain timeout is not committed, even if it is acknowledged later, so it is
  consumed again too.
  With `pause-if-no-requests=false`, the queue is not bounded, and the consumer keeps fetching records into memory
  for the whole drain.
  See [Kafka graceful shutdown](../kafka/receiving-kafka-records.md#graceful-shutdown), including what changed
  from the shutdown the Kafka connector used to handle itself.
- **What "sent" means depends on the connector.** Kafka, AMQP and Pulsar acknowledge an outgoing message once the broker accepted it.
  RabbitMQ does so only with `publish-confirms=true`: otherwise the drain only guarantees that every message was written
  before the channel closes.

Timeouts:

- **Phase timeout:** the default phase timeout assumes every wave of 10 channels may take the longest drain timeout.
  If the connectors' prepare-close or close calls are slow, or the computed timeout is too long for your platform's
  shutdown deadline, set `smallrye.messaging.graceful-shutdown.phase-timeout`.
- **Shutdown duration:** graceful shutdown is enabled by default, so the shutdown can take up to the drain timeout.
- **Outgoing channels:** the shutdown waits until the messages sent are acknowledged, up to the drain timeout.
  An emitter is completed during the drain: sending through it afterwards fails.
- **Request-reply:** Kafka request-reply emitters wait up to `reply.timeout` for pending replies; AMQP request-reply emitters
  complete pending replies immediately.

Message types and runtime channels:

- **Connector message types:** tracking wraps messages, which lose their connector-specific type.
  Consuming connector message types is deprecated: consume `Message<T>` and read the connector metadata instead
  (see [Custom Message types](model.md)).
  Until then, methods consuming the message types of the connectors of this project get them restored by the connector,
  and the drain still waits for them.
  A connector message type that no converter restores cannot be consumed: the messages are negatively acknowledged,
  and an error is logged (SRMSG00249). Consume `Message<T>`, or disable `graceful-shutdown` on the channel, until the
  connector provides a converter.
- **Channels created at runtime** with the [Channel Factory](channel-factory.md) don't support `concurrency`.
  If a connector fails to close such a channel, its name stays registered until the application restarts.

## Configuration Reference

| Property | Type | Default | Description |
|---|---|---|---|
Channel properties, which can also be set for every channel of a connector (`mp.messaging.connector.<connector>.<property>`):

| Property | Type | Default | Description |
|---|---|---|---|
| `graceful-shutdown` | boolean | `true` (the connector's default for incoming channels, `true` for every provided connector), unless `smallrye.messaging.graceful-shutdown.default.enabled` is `false` | Enable graceful shutdown for the channel |
| `graceful-shutdown.drain-timeout` | int (ms) | `10000`, unless `smallrye.messaging.graceful-shutdown.default.drain-timeout` is set | Maximum time to wait for in-flight messages to drain |

Global properties:

| Property | Type | Default | Description |
|---|---|---|---|
| `smallrye.messaging.graceful-shutdown.default.enabled` | boolean | `true` | Default of `graceful-shutdown` for the channels that set it neither on the channel nor on their connector |
| `smallrye.messaging.graceful-shutdown.default.drain-timeout` | int (ms) | `10000` | Default of `graceful-shutdown.drain-timeout` for the channels that set it neither on the channel nor on their connector |
| `smallrye.messaging.graceful-shutdown.phase-timeout` | int (ms) | longest channel drain timeout × ⌈channels / 10⌉ + 20 seconds | Maximum time each shutdown phase waits for all channels |
