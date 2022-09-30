package acme;

import java.util.concurrent.CompletionStage;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.smallrye.reactive.messaging.mqtt.server.MqttMessage;

@ApplicationScoped
public class MqttServerQuickstart {

    private static final Logger LOGGER = LoggerFactory.getLogger(MqttServerQuickstart.class);

    @Incoming("my-server")
    public CompletionStage<Void> source(MqttMessage message) {
        LOGGER.info("MQTT Message received {}", new String(message.getPayload()));
        return message.ack();
    }
}
