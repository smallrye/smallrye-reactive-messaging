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
       2. Connector prepare-close (e.g., flush commits)

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

Each phase has a global timeout of 30 seconds. If a channel fails within the timeout, the failure is logged and the shutdown continues with the remaining channels.
All methods are idempotent.

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

Each phase is idempotent — the CDI observer skips already-completed phases.

## Pausable Channels and Drain

Graceful shutdown is built on top of [Pausable Channels](pausable-channels.md).
When `graceful-shutdown` is enabled, the channel is automatically configured as pausable.

The `PausableChannel` interface provides `pauseAndDrain()` for programmatic use outside of shutdown:

``` java
{{ insert('shutdown/PauseAndDrainExample.java', 'pause-drain') }}
```

This is useful for operations that require no in-flight processing, such as seeking a Kafka consumer to a new position.

## Configuration Reference

| Property | Type | Default | Description |
|---|---|---|---|
| `graceful-shutdown` | boolean | `false` | Enable graceful shutdown for the channel |
| `graceful-shutdown.drain-timeout` | int (ms) | `10000` | Maximum time to wait for in-flight messages to drain |
