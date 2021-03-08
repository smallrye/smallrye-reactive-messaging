package io.smallrye.reactive.messaging.mqtt.hivemq;

import java.util.Map;

import org.eclipse.microprofile.reactive.messaging.spi.ConnectorLiteral;
import org.jboss.weld.environment.se.WeldContainer;

import io.smallrye.reactive.messaging.mqtt.MqttFactory;
import io.smallrye.reactive.messaging.mqtt.Sink;
import io.smallrye.reactive.messaging.mqtt.Source;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.mutiny.core.Vertx;

public class HiveMQMqttFactory implements MqttFactory {

    @Override
    public String connectorName() {
        return HiveMQMqttConnector.CONNECTOR_NAME;
    }

    @Override
    public boolean connectorIsReady(WeldContainer container) {
        return container.select(HiveMQMqttConnector.class, ConnectorLiteral.of(connectorName())).get()
                .isReady();
    }

    @Override
    public boolean connectorIsSourceReady(WeldContainer container) {
        return container
                .select(HiveMQMqttConnector.class, ConnectorLiteral.of(connectorName())).get().isSourceReady();
    }

    @Override
    public void clearClients() {
        HiveMQClients.clear();
    }

    @Override
    public Source createSource(Vertx vertx, Map<String, Object> config) {
        return new HiveMQMqttSource(new HiveMQMqttConnectorIncomingConfiguration(new MapBasedConfig(config)));
    }

    @Override
    public Sink createSink(Vertx vertx, Map<String, Object> config) {
        return new HiveMQMqttSink(vertx, new HiveMQMqttConnectorOutgoingConfiguration(new MapBasedConfig(config)));
    }
}
