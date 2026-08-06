package io.smallrye.reactive.messaging.providers.impl;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

    // --- Shutdown ---

    @Override
    public Uni<Void> shutdownIncoming(String channel) {
        String connectorName = registry.getIncomingConnectorName(channel);
        ConnectorLifecycle connector = connectorFactories.getInboundConnectors().get(connectorName);
        if (connector == null) {
            return Uni.createFrom().voidItem();
        }

        return drainChannel(channel)
                .emitOn(Infrastructure.getDefaultWorkerPool())
                .call(() -> Uni.createFrom().completionStage(() -> connector.preShutdownIncoming(channel)))
                .call(() -> Uni.createFrom().completionStage(() -> connector.shutdownIncoming(channel)));
    }

    @Override
    public Uni<Void> shutdownOutgoing(String channel) {
        String connectorName = registry.getOutgoingConnectorName(channel);
        ConnectorLifecycle connector = connectorFactories.getOutboundConnectors().get(connectorName);
        if (connector == null) {
            return Uni.createFrom().voidItem();
        }

        return drainChannel(channel)
                .emitOn(Infrastructure.getDefaultWorkerPool())
                .call(() -> Uni.createFrom().completionStage(() -> connector.preShutdownOutgoing(channel)))
                .call(() -> Uni.createFrom().completionStage(() -> connector.shutdownOutgoing(channel)));
    }

    public void terminate(
            @Observes @Priority(40) @BeforeDestroyed(ApplicationScoped.class) Object event) {
        // Shutdown incoming channels first (stop consuming before stopping producing)
        try {
            List<Uni<Void>> list = registry.getIncomingNames().stream()
                    .map(channel -> shutdownIncoming(channel)
                            .runSubscriptionOn(Infrastructure.getDefaultWorkerPool()))
                    .toList();
            if (!list.isEmpty()) {
                Uni.join().all(list).usingConcurrencyOf(10)
                        .andCollectFailures()
                        .await().atMost(Duration.ofSeconds(30));
            }
        } catch (Exception e) {
            log.warn("Error during incoming channel shutdown", e);
        }

        // Then shutdown outgoing channels
        try {
            List<Uni<Void>> list = registry.getOutgoingNames().stream()
                    .map(channel -> shutdownOutgoing(channel)
                            .runSubscriptionOn(Infrastructure.getDefaultWorkerPool()))
                    .toList();
            if (!list.isEmpty()) {
                Uni.join().all(list).usingConcurrencyOf(10)
                        .andCollectFailures()
                        .await().atMost(Duration.ofSeconds(30));
            }
        } catch (Exception e) {
            log.warn("Error during outgoing channel shutdown", e);
        }

        // Finally, terminate connector-wide resources
        terminateConnectors();
    }

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
            log.warnf(e, "Failed to drain channel '%s' during shutdown", channel);
            return Uni.createFrom().voidItem();
        }
    }
}
