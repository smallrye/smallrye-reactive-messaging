package io.smallrye.reactive.messaging.providers.impl;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.BeforeDestroyed;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Any;
import jakarta.inject.Inject;

import org.jboss.logging.Logger;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.reactive.messaging.ChannelLifecycleManager;
import io.smallrye.reactive.messaging.ChannelRegistry;
import io.smallrye.reactive.messaging.PausableChannel;
import io.smallrye.reactive.messaging.connector.ConnectorLifecycle;

@ApplicationScoped
public class ChannelLifecycleManagerImpl implements ChannelLifecycleManager {

    private static final Logger log = Logger.getLogger(ChannelLifecycleManagerImpl.class);

    @Inject
    @Any
    ChannelRegistry registry;

    @Inject
    ConnectorFactories connectorFactories;

    private final AtomicBoolean prepareCloseIncomingDone = new AtomicBoolean();
    private final AtomicBoolean prepareCloseOutgoingDone = new AtomicBoolean();
    private final AtomicBoolean closeDone = new AtomicBoolean();

    // --- Prepare close incoming channels ---

    @Override
    public void prepareCloseIncoming() {
        if (!prepareCloseIncomingDone.compareAndSet(false, true)) {
            return;
        }

        try {
            List<Uni<Void>> list = registry.getIncomingNames().stream()
                    .map(channel -> prepareCloseIncoming(channel)
                            .runSubscriptionOn(Infrastructure.getDefaultWorkerPool()))
                    .toList();
            if (!list.isEmpty()) {
                Uni.join().all(list).usingConcurrencyOf(10)
                        .andCollectFailures()
                        .await().atMost(Duration.ofSeconds(30));
            }
        } catch (Exception e) {
            log.warn("Error preparing close of incoming channels", e);
        }
    }

    // --- Prepare close outgoing channels ---

    @Override
    public void prepareCloseOutgoing() {
        if (!prepareCloseOutgoingDone.compareAndSet(false, true)) {
            return;
        }

        try {
            List<Uni<Void>> list = registry.getOutgoingNames().stream()
                    .map(channel -> prepareCloseOutgoing(channel)
                            .runSubscriptionOn(Infrastructure.getDefaultWorkerPool()))
                    .toList();
            if (!list.isEmpty()) {
                Uni.join().all(list).usingConcurrencyOf(10)
                        .andCollectFailures()
                        .await().atMost(Duration.ofSeconds(30));
            }
        } catch (Exception e) {
            log.warn("Error preparing close of outgoing channels", e);
        }
    }

    // --- Close clients + terminate connectors ---

    @Override
    public void close() {
        if (!closeDone.compareAndSet(false, true)) {
            return;
        }

        try {
            List<Uni<Void>> list = registry.getIncomingNames().stream()
                    .map(channel -> closeIncoming(channel)
                            .runSubscriptionOn(Infrastructure.getDefaultWorkerPool()))
                    .toList();
            if (!list.isEmpty()) {
                Uni.join().all(list).usingConcurrencyOf(10)
                        .andCollectFailures()
                        .await().atMost(Duration.ofSeconds(30));
            }
        } catch (Exception e) {
            log.warn("Error closing incoming channels", e);
        }

        try {
            List<Uni<Void>> list = registry.getOutgoingNames().stream()
                    .map(channel -> closeOutgoing(channel)
                            .runSubscriptionOn(Infrastructure.getDefaultWorkerPool()))
                    .toList();
            if (!list.isEmpty()) {
                Uni.join().all(list).usingConcurrencyOf(10)
                        .andCollectFailures()
                        .await().atMost(Duration.ofSeconds(30));
            }
        } catch (Exception e) {
            log.warn("Error closing outgoing channels", e);
        }

        terminateConnectors();
    }

    /**
     * CDI fallback: runs all three phases during CDI container destruction.
     * Each phase is idempotent — if an external orchestrator (e.g. Quarkus service graph)
     * already called individual phases, they are skipped here.
     */
    public void onBeforeDestroyed(
            @Observes @Priority(40) @BeforeDestroyed(ApplicationScoped.class) Object event) {
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

        return drainChannel(channel)
                .emitOn(Infrastructure.getDefaultWorkerPool())
                .call(() -> Uni.createFrom().completionStage(() -> connector.prepareCloseIncoming(channel)));
    }

    @Override
    public Uni<Void> prepareCloseOutgoing(String channel) {
        String connectorName = registry.getOutgoingConnectorName(channel);
        ConnectorLifecycle connector = connectorFactories.getOutboundConnectors().get(connectorName);
        if (connector == null) {
            return Uni.createFrom().voidItem();
        }

        return drainChannel(channel)
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

        return Uni.createFrom().completionStage(() -> connector.closeIncoming(channel))
                .runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    @Override
    public Uni<Void> closeOutgoing(String channel) {
        String connectorName = registry.getOutgoingConnectorName(channel);
        ConnectorLifecycle connector = connectorFactories.getOutboundConnectors().get(connectorName);
        if (connector == null) {
            return Uni.createFrom().voidItem();
        }

        return Uni.createFrom().completionStage(() -> connector.closeOutgoing(channel))
                .runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    // --- Connector termination ---

    private void terminateConnectors() {
        Set<ConnectorLifecycle> connectors = new HashSet<>();
        connectors.addAll(connectorFactories.getInboundConnectors().values());
        connectors.addAll(connectorFactories.getOutboundConnectors().values());

        try {
            List<Uni<Void>> list = connectors.stream()
                    .map(c -> Uni.createFrom().completionStage(c::terminate)
                            .runSubscriptionOn(Infrastructure.getDefaultWorkerPool()))
                    .toList();
            if (!list.isEmpty()) {
                Uni.join().all(list).usingConcurrencyOf(10)
                        .andCollectFailures()
                        .await().atMost(Duration.ofSeconds(30));
            }
        } catch (Exception e) {
            log.warn("Error during connector termination", e);
        }
    }

    // --- Channel drain ---

    private Uni<Void> drainChannel(String channel) {
        PausableChannel pausable = registry.getPausable(channel);
        if (pausable == null) {
            return Uni.createFrom().voidItem();
        }
        try {
            Duration timeout = pausable.getDrainTimeout();
            return pausable.pauseAndDrain()
                    .ifNoItem().after(timeout).fail()
                    .onFailure().recoverWithNull();
        } catch (Exception e) {
            log.warnf(e, "Failed to drain channel '%s'", channel);
            return Uni.createFrom().voidItem();
        }
    }
}
