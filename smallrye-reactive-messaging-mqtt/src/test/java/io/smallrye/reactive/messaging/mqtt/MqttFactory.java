package io.smallrye.reactive.messaging.mqtt;

import java.util.Map;

import org.jboss.weld.environment.se.WeldContainer;

import io.vertx.mutiny.core.Vertx;

public interface MqttFactory {
    String connectorName();

    boolean connectorIsReady(WeldContainer container);

    boolean connectorIsSourceReady(WeldContainer container);

    void clearClients();

    Source createSource(Vertx vertx, Map<String, Object> config);

    Sink createSink(Vertx vertx, Map<String, Object> config);
}
