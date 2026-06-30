# PausableChannel drain — implementation plan

## Current state

- `PausableChannel` has `pauseAndDrain()` returning `Uni<Void>` — pauses and waits for in-flight messages to complete
- `PausableChannelDecorator.PauserChannel` implements WIP tracking via message wrapping (intercepts ack/nack)
- WIP tracking happens AFTER the buffer, so drain works with any buffer strategy
- `ConsumerSeekTest` uses `pauseAndDrain()` for the seek workflow
- Channels require `pausable=true` to get a `PausableChannel` registered
- WIP tracking is always active on pausable channels

## Remaining steps

### 1. Default `pausable=true`

Change the default of `pausable` from `false` to `true`. Channels that don't want it can opt out with `pausable=false`.

**Files:**
- `smallrye-reactive-messaging-provider/.../ConfiguredChannelFactory.java` — change the condition that checks for `pausable` config to default to `true`

### 2. Gate WIP tracking on `graceful-shutdown=true`

The message wrapping in `trackMessage()` adds overhead on every message (ack/nack interception). This should only be active when `graceful-shutdown=true` (which is the default).

When `graceful-shutdown=false`:
- `PauserChannel` is still registered (pause/resume/clearBuffer work)
- `pauseAndDrain()` pauses but completes immediately (no WIP tracking)
- No message wrapping overhead

**Files:**
- `PausableChannelDecorator` — accept `gracefulShutdown` flag per channel, conditionally apply `trackMessage` transform
- `ConfiguredChannelFactory` — pass `graceful-shutdown` config value to the decorator

### 3. Use `pauseAndDrain` in connector graceful shutdown

Replace the private `waitForProcessing()` polling loops in Kafka commit handlers with `pauseAndDrain()` on the `PausableChannel`. The connector shutdown path becomes:

```java
pausableChannel.pauseAndDrain().await().atMost(Duration.ofMillis(timeout));
commitAllAndAwait();
```

**Files:**
- `KafkaThrottledLatestProcessedCommit.terminate()` — remove `waitForProcessing()` loop
- `KafkaCheckpointCommit.terminate()` — remove `waitForProcessing()` loop  
- `KafkaShareGroupCommit.terminate()` — remove `waitForProcessing()` loop
- These handlers need access to the `PausableChannel` — pass via factory or lookup from `ChannelRegistry`

### 4. Remove Kafka-specific `drainProcessing` from `KafkaCommitHandler`

Once graceful shutdown uses `PausableChannel.pauseAndDrain()`, the `drainProcessing()` default method on `KafkaCommitHandler` and its override in `KafkaLatestCommit` are no longer needed.

**Files:**
- `KafkaCommitHandler.java` — remove `drainProcessing()` default method
- `KafkaLatestCommit.java` — remove `drainProcessing()` override, `received()` override, `seekPendingPartitions`, WIP counter

### 5. Tests

- Update existing `PausableChannelTest` to verify `pauseAndDrain()` behavior
- Add a test without `pausable=true` config to verify auto-registration
- Verify `graceful-shutdown=false` skips WIP tracking
