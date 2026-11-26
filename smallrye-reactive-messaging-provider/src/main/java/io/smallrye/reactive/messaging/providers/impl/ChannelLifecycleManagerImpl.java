package io.smallrye.reactive.messaging.providers.impl;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.BeforeDestroyed;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.Config;
import org.jboss.logging.Logger;

import io.smallrye.mutiny.TimeoutException;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.reactive.messaging.ChannelLifecycleManager;
import io.smallrye.reactive.messaging.ChannelRegistry;
import io.smallrye.reactive.messaging.PausableChannel;
import io.smallrye.reactive.messaging.PausableChannelConfiguration;
import io.smallrye.reactive.messaging.connector.ConnectorLifecycle;
import io.smallrye.reactive.messaging.connector.InboundConnector;
import io.smallrye.reactive.messaging.connector.OutboundConnector;
import io.smallrye.reactive.messaging.providers.extension.CompositePausableChannel;
import io.smallrye.reactive.messaging.providers.extension.PausableChannelDecorator;
import io.smallrye.reactive.messaging.providers.i18n.ProviderLogging;

@ApplicationScoped
public class ChannelLifecycleManagerImpl implements ChannelLifecycleManager {

    private static final Logger log = Logger.getLogger(ChannelLifecycleManagerImpl.class);

    /**
     * Global property overriding the timeout of each shutdown phase, in milliseconds.
     */
    public static final String PHASE_TIMEOUT_PROPERTY = "smallrye.messaging.graceful-shutdown.phase-timeout";

    /**
     * The default of {@code graceful-shutdown.drain-timeout}, when
     * {@link PausableChannelConfiguration#GRACEFUL_SHUTDOWN_DEFAULT_DRAIN_TIMEOUT_PROPERTY} is not set.
     */
    static final Duration DEFAULT_DRAIN_TIMEOUT = Duration.ofMillis(PausableChannelDecorator.DEFAULT_DRAIN_TIMEOUT_MS);

    /**
     * Added to the longest drain timeout, for the connectors' prepare-close and close calls.
     */
    static final Duration PHASE_TIMEOUT_MARGIN = Duration.ofSeconds(20);

    /**
     * The number of channels (or connectors) handled at the same time in each shutdown phase.
     */
    static final int PHASE_CONCURRENCY = 10;

    /**
     * Priority of the fallback that runs the shutdown phases when the CDI container is destroyed. It is notified before
     * the other {@code @BeforeDestroyed} observers of SmallRye Reactive Messaging, which close what the channels use:
     * the AWS clients ({@code SqsManager}, {@code SnsManager}, 50), the worker pools ({@code WorkerPoolRegistry}, 100)
     * and the Vert.x instance ({@code ExecutionHolder}, 200).
     * <p>
     * Runtimes that run the phases themselves, earlier in their shutdown, do not depend on it: each phase runs once,
     * so the fallback then has nothing left to do.
     */
    public static final int FALLBACK_PRIORITY = 40;

    @Inject
    @Any
    ChannelRegistry registry;

    @Inject
    ConnectorFactories connectorFactories;

    @Inject
    Instance<Config> configInstance;

    private final AtomicBoolean prepareCloseIncomingDone = new AtomicBoolean();
    private final AtomicBoolean prepareCloseOutgoingDone = new AtomicBoolean();
    private final AtomicBoolean closeDone = new AtomicBoolean();

    // The connectors of the channels closed one by one (e.g. runtime channels closed before shutdown): closing a channel
    // unregisters it, so the registry no longer knows these connectors when shutdown terminates the connectors in use.
    private final Set<String> closedChannelConnectors = ConcurrentHashMap.newKeySet();

    // --- Prepare close incoming channels ---

    // The blocking variants create no Uni when there is nothing to do: a phase already started, or no channel (after a
    // failed startup). The CDI fallback calls them when the container is destroyed, when the Mutiny context
    // propagation, which decorates Uni operations, may already be shut down.

    @Override
    public void prepareCloseIncoming() {
        if (prepareCloseIncomingDone.compareAndSet(false, true)) {
            awaitPhase(ProviderLogging.log::prepareCloseIncomingFailed, this::prepareIncomingCalls);
        }
    }

    @Override
    public Uni<Void> prepareCloseIncomingAsync() {
        return once(prepareCloseIncomingDone,
                () -> runPhase(ProviderLogging.log::prepareCloseIncomingFailed, this::prepareIncomingCalls));
    }

    private List<Uni<Void>> prepareIncomingCalls() {
        return onWorkerThreads(registry.getIncomingNames(), this::prepareCloseIncoming);
    }

    // --- Prepare close outgoing channels ---

    @Override
    public void prepareCloseOutgoing() {
        if (prepareCloseOutgoingDone.compareAndSet(false, true)) {
            awaitPhase(ProviderLogging.log::prepareCloseOutgoingFailed, this::prepareOutgoingCalls);
        }
    }

    @Override
    public Uni<Void> prepareCloseOutgoingAsync() {
        return once(prepareCloseOutgoingDone,
                () -> runPhase(ProviderLogging.log::prepareCloseOutgoingFailed, this::prepareOutgoingCalls));
    }

    private List<Uni<Void>> prepareOutgoingCalls() {
        return onWorkerThreads(registry.getOutgoingNames(), this::prepareCloseOutgoing);
    }

    // --- Close clients + terminate connectors ---

    @Override
    public void close() {
        if (closeDone.compareAndSet(false, true)) {
            Set<String> usedConnectors = usedConnectors();
            awaitPhase(ProviderLogging.log::closeIncomingFailed, this::closeIncomingCalls);
            awaitPhase(ProviderLogging.log::closeOutgoingFailed, this::closeOutgoingCalls);
            awaitPhase(ProviderLogging.log::terminateConnectorsFailed, () -> terminateCalls(usedConnectors));
        }
    }

    @Override
    public Uni<Void> closeAsync() {
        return once(closeDone, () -> {
            Set<String> usedConnectors = usedConnectors();
            return runPhase(ProviderLogging.log::closeIncomingFailed, this::closeIncomingCalls)
                    .chain(() -> runPhase(ProviderLogging.log::closeOutgoingFailed, this::closeOutgoingCalls))
                    .chain(() -> runPhase(ProviderLogging.log::terminateConnectorsFailed,
                            () -> terminateCalls(usedConnectors)));
        });
    }

    /**
     * Runs the phase when subscribed, unless it already started.
     */
    private static Uni<Void> once(AtomicBoolean started, Supplier<Uni<Void>> phase) {
        return Uni.createFrom().deferred(() -> started.compareAndSet(false, true)
                ? phase.get()
                : Uni.createFrom().voidItem());
    }

    /**
     * @return a call per channel, subscribed on a worker thread
     */
    private static List<Uni<Void>> onWorkerThreads(Collection<String> channels, Function<String, Uni<Void>> call) {
        return channels.stream()
                .map(channel -> call.apply(channel).runSubscriptionOn(Infrastructure.getDefaultWorkerPool()))
                .toList();
    }

    /**
     * Taken before the channels are closed, which unregisters them: the connectors actually in use, including those of
     * channels already closed one by one. Connectors without channels may never have been created (their beans are
     * created lazily), so they are left alone.
     */
    private Set<String> usedConnectors() {
        Set<String> usedConnectors = new LinkedHashSet<>(registry.getConnectorNames().values());
        usedConnectors.addAll(closedChannelConnectors);
        return usedConnectors;
    }

    private List<Uni<Void>> closeIncomingCalls() {
        return onWorkerThreads(registry.getIncomingNames(), this::closeIncoming);
    }

    private List<Uni<Void>> closeOutgoingCalls() {
        return onWorkerThreads(registry.getOutgoingNames(), this::closeOutgoing);
    }

    /**
     * CDI fallback: runs all three phases during CDI container destruction.
     * Each phase runs at most once: if an external orchestrator (e.g. Quarkus service graph) already started a phase,
     * it is skipped here. The guard is set when a phase starts, so a phase that is still running is skipped too.
     */
    // Notified even if the manager was never created: nothing else may have used it (e.g. without a runtime calling the
    // phases), and the connectors are only closed by these phases.
    public void onBeforeDestroyed(
            @Observes @Priority(FALLBACK_PRIORITY) @BeforeDestroyed(ApplicationScoped.class) Object event) {
        prepareCloseIncoming();
        prepareCloseOutgoing();
        close();
    }

    // --- Per-channel prepare close (drain + connector prepare-close) ---

    @Override
    public Uni<Void> prepareCloseIncoming(String channel) {
        String connectorName = registry.getIncomingConnectorName(channel);
        ConnectorLifecycle connector = connectorFactories.getInboundConnectors().get(connectorName);
        if (connector == null) {
            return Uni.createFrom().voidItem();
        }

        // A channel configured with concurrency is drained as a whole (composite pausable under its name),
        // then each indexed connector channel is prepared.
        // The channel is paused right away, not when the Uni is subscribed: a shutdown phase pauses every incoming
        // channel at once, even if it waits for PHASE_CONCURRENCY of them at a time.
        return drainChannel(channel)
                .emitOn(Infrastructure.getDefaultWorkerPool())
                .call(() -> forEachConnectorChannel(incomingConnectorChannels(channel), connector::prepareCloseIncoming));
    }

    @Override
    public Uni<Void> prepareCloseOutgoing(String channel) {
        String connectorName = registry.getOutgoingConnectorName(channel);
        ConnectorLifecycle connector = connectorFactories.getOutboundConnectors().get(connectorName);
        if (connector == null) {
            return Uni.createFrom().voidItem();
        }

        // Drained when subscribed: completing the emitters may block, for example while a request-reply emitter waits
        // for its pending replies, so it runs on the worker thread of the call, PHASE_CONCURRENCY channels at a time,
        // bounded by the phase timeout.
        return Uni.createFrom().deferred(() -> drainChannel(channel))
                .emitOn(Infrastructure.getDefaultWorkerPool())
                .call(() -> Uni.createFrom().completionStage(() -> connector.prepareCloseOutgoing(channel)));
    }

    // --- Per-channel close ---

    @Override
    public Uni<Void> closeIncoming(String channel) {
        String connectorName = registry.getIncomingConnectorName(channel);
        ConnectorLifecycle connector = connectorFactories.getInboundConnectors().get(connectorName);
        if (connector == null) {
            return Uni.createFrom().voidItem();
        }

        closedChannelConnectors.add(connectorName);
        return forEachConnectorChannel(incomingConnectorChannels(channel), connector::closeIncoming)
                // The channel is closed: forget it, so a channel created at runtime can be created again.
                .invoke(() -> unregisterIncoming(channel))
                .runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    private void unregisterIncoming(String channel) {
        if (registry instanceof InternalChannelRegistry internal) {
            internal.unregisterIncoming(channel);
        }
    }

    private void unregisterOutgoing(String channel) {
        if (registry instanceof InternalChannelRegistry internal) {
            internal.unregisterOutgoing(channel);
        }
    }

    /**
     * @return the channel names the connector uses for the given incoming channel: the indexed channels for a channel
     *         configured with {@code concurrency}, the channel itself otherwise
     */
    private List<String> incomingConnectorChannels(String channel) {
        if (registry instanceof InternalChannelRegistry internal) {
            return internal.getIncomingConnectorChannels(channel);
        }
        return List.of(channel);
    }

    private static Uni<Void> forEachConnectorChannel(List<String> connectorChannels,
            Function<String, CompletionStage<Void>> hook) {
        if (connectorChannels.size() == 1) {
            String connectorChannel = connectorChannels.get(0);
            return Uni.createFrom().completionStage(() -> hook.apply(connectorChannel));
        }
        List<Uni<Void>> calls = connectorChannels.stream()
                .map(connectorChannel -> Uni.createFrom().completionStage(() -> hook.apply(connectorChannel)))
                .toList();
        return Uni.join().all(calls).andCollectFailures().replaceWithVoid();
    }

    @Override
    public Uni<Void> closeOutgoing(String channel) {
        String connectorName = registry.getOutgoingConnectorName(channel);
        ConnectorLifecycle connector = connectorFactories.getOutboundConnectors().get(connectorName);
        if (connector == null) {
            return Uni.createFrom().voidItem();
        }

        closedChannelConnectors.add(connectorName);
        return Uni.createFrom().completionStage(() -> connector.closeOutgoing(channel))
                // The channel is closed: forget it, so a channel created at runtime can be created again.
                .invoke(() -> unregisterOutgoing(channel))
                .runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    /**
     * Runs the calls of a shutdown phase, {@link #PHASE_CONCURRENCY} at a time, and waits for them, at most for the
     * {@linkplain #phaseTimeout(int) phase timeout}. Failures are logged, so the next phases still run. Without calls,
     * no {@link Uni} is created.
     */
    private void awaitPhase(Consumer<Throwable> onFailure, Supplier<List<Uni<Void>>> calls) {
        try {
            List<Uni<Void>> list = calls.get();
            if (!list.isEmpty()) {
                join(list).await().indefinitely();
            }
        } catch (Exception e) {
            onFailure.accept(e);
        }
    }

    /**
     * The non-blocking variant of {@link #awaitPhase(Consumer, Supplier)}: runs the calls when subscribed, and completes
     * when they complete, at most after the phase timeout. Failures, including the timeout, are logged and do not fail
     * the returned {@link Uni}, so the next phases still run.
     */
    private Uni<Void> runPhase(Consumer<Throwable> onFailure, Supplier<List<Uni<Void>>> calls) {
        return Uni.createFrom().<Void> deferred(() -> {
            List<Uni<Void>> list = calls.get();
            if (list.isEmpty()) {
                return Uni.createFrom().voidItem();
            }
            return join(list);
        }).onFailure().recoverWithItem(failure -> {
            onFailure.accept(failure);
            return null;
        });
    }

    /**
     * Joins the calls of a phase, {@link #PHASE_CONCURRENCY} at a time, failing after the phase timeout.
     */
    private Uni<Void> join(List<Uni<Void>> calls) {
        return Uni.join().all(calls).usingConcurrencyOf(PHASE_CONCURRENCY)
                .andCollectFailures()
                .ifNoItem().after(phaseTimeout(calls.size())).fail()
                .replaceWithVoid();
    }

    /**
     * The timeout of a shutdown phase: the {@value #PHASE_TIMEOUT_PROPERTY} property if set, otherwise computed from
     * the drain timeouts of the pausable channels, see {@link #phaseTimeout(Optional, Collection, int)}.
     *
     * @param calls the number of calls in the phase
     * @return the phase timeout
     */
    private Duration phaseTimeout(int calls) {
        return phaseTimeout(configuredPhaseTimeout(), defaultDrainTimeout(), registry.getPausableChannels().values(),
                calls);
    }

    /**
     * The default drain timeout, assumed when no channel is pausable. Read as {@link #configuredPhaseTimeout()}: if
     * the configuration cannot be read at shutdown, {@link #DEFAULT_DRAIN_TIMEOUT} is used.
     */
    private Duration defaultDrainTimeout() {
        return readConfig(PausableChannelConfiguration.GRACEFUL_SHUTDOWN_DEFAULT_DRAIN_TIMEOUT_PROPERTY,
                config -> Duration.ofMillis(PausableChannelDecorator.defaultDrainTimeoutMs(config)), DEFAULT_DRAIN_TIMEOUT);
    }

    /**
     * The value of the {@value #PHASE_TIMEOUT_PROPERTY} property, if it can be read. The phases run at shutdown, when the
     * configuration may already be gone (for example, released before the CDI container is closed): the computed
     * timeout is used then, as failing here would skip the whole phase.
     */
    private Optional<Integer> configuredPhaseTimeout() {
        return readConfig(PHASE_TIMEOUT_PROPERTY,
                config -> config.getOptionalValue(PHASE_TIMEOUT_PROPERTY, Integer.class), Optional.empty());
    }

    /**
     * Reads the configuration at shutdown, or returns the fallback if it is not available anymore.
     */
    private <T> T readConfig(String property, Function<Config, T> read, T fallback) {
        if (configInstance == null || !configInstance.isResolvable()) {
            return fallback;
        }
        try {
            return read.apply(configInstance.get());
        } catch (RuntimeException e) {
            log.debugf(e, "Cannot read %s at shutdown, using its default", property);
            return fallback;
        }
    }

    /**
     * The calls of a phase run {@link #PHASE_CONCURRENCY} at a time, so they run in {@code ceil(calls / 10)} waves, and
     * each wave can take up to the longest drain timeout. The default timeout covers all the waves, plus a margin for the
     * connector calls. It is only an upper bound: a phase ends as soon as its calls complete.
     *
     * @param configuredMillis the value of the {@value #PHASE_TIMEOUT_PROPERTY} property, if set
     * @param defaultDrain the drain timeout assumed when no channel is pausable
     * @param pausables the pausable channels to drain
     * @param calls the number of calls in the phase
     * @return the configured timeout, or the longest drain timeout times the number of waves, plus
     *         {@link #PHASE_TIMEOUT_MARGIN}
     */
    static Duration phaseTimeout(Optional<Integer> configuredMillis, Duration defaultDrain,
            Collection<PausableChannel> pausables, int calls) {
        if (configuredMillis.isPresent()) {
            return Duration.ofMillis(configuredMillis.get());
        }
        Duration longestDrain = pausables.stream()
                .map(PausableChannel::getDrainTimeout)
                .max(Duration::compareTo)
                .orElse(defaultDrain);
        int waves = Math.max(1, (calls + PHASE_CONCURRENCY - 1) / PHASE_CONCURRENCY);
        return longestDrain.multipliedBy(waves).plus(PHASE_TIMEOUT_MARGIN);
    }

    // --- Connector termination ---

    private List<Uni<Void>> terminateCalls(Collection<String> usedConnectors) {
        return connectorsToTerminate(usedConnectors, connectorFactories.getInboundConnectors(),
                connectorFactories.getOutboundConnectors()).stream()
                .map(c -> Uni.createFrom().completionStage(c::terminate)
                        .runSubscriptionOn(Infrastructure.getDefaultWorkerPool()))
                .toList();
    }

    /**
     * The connectors to terminate: those of the given names, each once.
     * <p>
     * A connector implementing both {@link InboundConnector} and {@link OutboundConnector} (e.g. Kafka) is in both maps
     * under the same name. If the inbound connector of a name is also an {@link OutboundConnector}, it is the outbound
     * connector of that name too (another outbound bean with the same name would have been rejected at startup), so it
     * is terminated once. This doesn't rely on the equality of the CDI client proxies.
     *
     * @param usedConnectors the names of the connectors used by channels
     * @param inbound the inbound connectors, by name
     * @param outbound the outbound connectors, by name
     * @return the connectors to terminate
     */
    static List<ConnectorLifecycle> connectorsToTerminate(Collection<String> usedConnectors,
            Map<String, ? extends InboundConnector> inbound, Map<String, ? extends OutboundConnector> outbound) {
        List<ConnectorLifecycle> connectors = new ArrayList<>();
        for (String name : new LinkedHashSet<>(usedConnectors)) {
            InboundConnector in = inbound.get(name);
            OutboundConnector out = outbound.get(name);
            if (in != null) {
                connectors.add(in);
            }
            if (out != null && !(in instanceof OutboundConnector)) {
                connectors.add(out);
            }
        }
        return connectors;
    }

    // --- Channel drain ---

    private Uni<Void> drainChannel(String channel) {
        PausableChannel pausable = registry.getPausable(channel);
        if (pausable == null) {
            return Uni.createFrom().voidItem();
        }
        try {
            Duration timeout = pausable.getDrainTimeout();
            ProviderLogging.log.drainingChannel(channel, inFlight(pausable), timeout);
            long start = System.nanoTime();
            return pausable.pauseAndDrain()
                    .ifNoItem().after(timeout).fail()
                    .invoke(() -> ProviderLogging.log.channelDrained(channel,
                            TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)))
                    .onFailure().invoke(failure -> {
                        if (failure instanceof TimeoutException) {
                            ProviderLogging.log.drainTimedOut(channel, timeout, inFlight(pausable));
                        } else {
                            ProviderLogging.log.drainFailed(channel, failure);
                        }
                    })
                    .onFailure().recoverWithNull();
        } catch (Exception e) {
            ProviderLogging.log.drainFailed(channel, e);
            return Uni.createFrom().voidItem();
        }
    }

    private static String inFlight(PausableChannel pausable) {
        int count = CompositePausableChannel.inFlightCount(pausable);
        return count < 0 ? "unknown" : String.valueOf(count);
    }
}
