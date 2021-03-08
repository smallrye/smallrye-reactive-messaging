package io.smallrye.reactive.messaging.mqtt;

import java.util.Map;

import org.eclipse.microprofile.reactive.messaging.spi.ConnectorLiteral;
import org.jboss.weld.environment.se.WeldContainer;

import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.mutiny.core.Vertx;

public class VertxMqttFactory implements MqttFactory {

    @Override
    public String connectorName() {
        return MqttConnector.CONNECTOR_NAME;
    }

    @Override
    public boolean connectorIsReady(WeldContainer container) {
        return container.select(MqttConnector.class, ConnectorLiteral.of(connectorName())).get()
                .isReady();
    }

    @Override
    public boolean connectorIsSourceReady(WeldContainer container) {
        return container
                .select(MqttConnector.class, ConnectorLiteral.of("smallrye-mqtt")).get().isSourceReady();
    }

    @Override
    public void clearClients() {
        Clients.clear();
    }

    @Override
    public Source createSource(Vertx vertx, Map<String, Object> config) {
        return new MqttSource(vertx, new MqttConnectorIncomingConfiguration(new MapBasedConfig(config)));
    }

    @Override
    public Sink createSink(Vertx vertx, Map<String, Object> config) {
        return new MqttSink(vertx, new MqttConnectorOutgoingConfiguration(new MapBasedConfig(config)));
    }
}
