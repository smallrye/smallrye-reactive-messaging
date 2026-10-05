package mqtt.outbound;

import java.util.Map;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.smallrye.reactive.messaging.mqtt.MqttMessage;

@ApplicationScoped
public class MqttRequestResponseHandler {

    @Incoming("requests")
    @Outgoing("responses")
    public MqttMessage<String> handle(MqttMessage<byte[]> request) {
        String result = process(request.getPayload());

        // ofResponse() sends the message on the `Response Topic` carried by the request and
        // copies its `Correlation Data`, so that the requester can match request and response.
        return MqttMessage.ofResponse(request, result, MqttQoS.AT_LEAST_ONCE,
                Map.of("version", "2"));
    }

    private String process(byte[] payload) {
        return new String(payload).toUpperCase();
    }
}
