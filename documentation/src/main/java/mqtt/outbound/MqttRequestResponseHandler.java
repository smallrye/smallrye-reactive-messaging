package mqtt.outbound;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.smallrye.reactive.messaging.mqtt.MqttMessage;

@ApplicationScoped
public class MqttRequestResponseHandler {

    @Incoming("requests")
    @Outgoing("responses")
    public MqttMessage<byte[]> handle(MqttMessage<byte[]> request) {
        // Process the request payload (request.getPayload()) here...

        MqttProperties properties = new MqttProperties();
        properties.add(new MqttProperties.UserProperty("version", "2"));
        properties.add(new MqttProperties.UserProperty("result", "0"));

        // ofResponse() targets the Response Topic carried by the request and
        // copies the Correlation Data, so the requester can match the reply.
        return MqttMessage.ofResponse(request, null, MqttQoS.AT_MOST_ONCE, properties);
    }
}
