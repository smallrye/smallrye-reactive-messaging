# Graceful Shutdown

When a reactive messaging application shuts down, messages may still be in-flight — delivered to a processing method but not yet acknowledged.
By default, the framework cancels the reactive streams and closes connectors immediately, which can lead to message loss or duplicate processing on restart.

Graceful shutdown addresses this by:

1. **Pausing** channels to stop receiving new messages
2. **Draining** in-flight messages, waiting for them to be acknowledged or negatively acknowledged
3. **Shutting down** connectors per-channel in a controlled sequence

## Enabling Graceful Shutdown

To enable graceful shutdown on a channel, set the `graceful-shutdown` property to `true`:

```properties
mp.messaging.incoming.data.graceful-shutdown=true
```

This works for both incoming and outgoing channels:

```properties
mp.messaging.outgoing.out.graceful-shutdown=true
```

### Default Value

For incoming channels, the default depends on the connector.
A connector can declare its own default for `graceful-shutdown`; the framework uses it when the property is not set.
The Kafka connector enables graceful shutdown by default, while the other connectors disable it.
For outgoing channels, graceful shutdown is disabled unless the property is set.

The value is resolved in this order: the channel property, the connector-level property (`mp.messaging.connector.<connector>.graceful-shutdown`), then the connector's default.
To disable graceful shutdown for every Kafka channel:

```properties
mp.messaging.connector.smallrye-kafka.graceful-shutdown=false
```

!!! note "Implementing a connector"
    Connectors that declare a default for `graceful-shutdown` should override `InboundConnector#isGracefulShutdownEnabled(Config)`
    so the declared default applies, for example by delegating to the generated configuration class.

### Drain Timeout

The drain timeout controls how long the framework waits for in-flight messages to complete.
If the timeout is reached, the shutdown proceeds regardless.
The default is 10 seconds (10000 milliseconds).

```properties
mp.messaging.incoming.data.graceful-shutdown.drain-timeout=15000
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

### Outgoing Channels (Emitters)

When graceful shutdown is enabled on an outgoing channel backed by an `Emitter`, the emitter is **completed** during shutdown.
This flushes any buffered messages downstream to the connector before closing.

``` java
{{ insert('shutdown/EmitterShutdownExample.java', 'emitter') }}
```

After the emitter is completed and all buffered messages are flushed, the connector's pre-shutdown and shutdown hooks are called.

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
       1. Complete emitter and drain buffered messages
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

## Limitations

What the drain waits for:

- **Acknowledgment strategy:** in-flight messages are tracked through their acknowledgment.
  With `PRE_PROCESSING` or `NONE` acknowledgment, messages leave the in-flight count before they are processed,
  so the drain does not wait for their processing.
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
  With connectors that acknowledge on receipt, such as RabbitMQ with `auto-acknowledgement=true` or MQTT with QoS 0,
  they are lost.
- **What "sent" means depends on the connector.** Kafka, AMQP and Pulsar acknowledge an outgoing message once the broker accepted it.
  RabbitMQ does so only with `publish-confirms=true`: otherwise the drain only guarantees that every message was written
  before the channel closes.

Timeouts:

- **Phase timeout:** the default phase timeout assumes every wave of 10 channels may take the longest drain timeout.
  If the connectors' prepare-close or close calls are slow, or the computed timeout is too long for your platform's
  shutdown deadline, set `smallrye.messaging.graceful-shutdown.phase-timeout`.
- **Kafka:** graceful shutdown is enabled by default for Kafka incoming channels, so the shutdown can take up to the drain timeout.
  Kafka request-reply emitters wait up to `reply.timeout` for pending replies; AMQP request-reply emitters complete
  pending replies immediately.

Message types and runtime channels:

- **Connector message types:** tracking rebuilds messages, which lose their connector-specific type.
  Methods consuming `KafkaRecord`, `IncomingKafkaRecord`, `KafkaRecordBatch` or `IncomingKafkaRecordBatch` get it restored.
  Other connector message types, such as `AmqpMessage`, `IncomingRabbitMQMessage` or `PulsarIncomingMessage`, are not restored:
  consume `Message<T>` and read the connector metadata instead (see [Custom Message types](model.md)).
- **Channels created at runtime** with the [Channel Factory](channel-factory.md) don't support `concurrency`.
  If a connector fails to close such a channel, its name stays registered until the application restarts.

## Configuration Reference

| Property | Type | Default | Description |
|---|---|---|---|
| `graceful-shutdown` | boolean | connector-specific (`true` for Kafka incoming channels, `false` otherwise) | Enable graceful shutdown for the channel |
| `graceful-shutdown.drain-timeout` | int (ms) | `10000` | Maximum time to wait for in-flight messages to drain |

The timeout of each shutdown phase is a global property:

| Property | Type | Default | Description |
|---|---|---|---|
| `smallrye.messaging.graceful-shutdown.phase-timeout` | int (ms) | longest channel drain timeout × ⌈channels / 10⌉ + 20 seconds | Maximum time each shutdown phase waits for all channels |
