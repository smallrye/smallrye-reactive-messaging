package io.smallrye.reactive.messaging.example.eventclouds;

import io.smallrye.reactive.messaging.cloudevents.CloudEventMessage;
import io.smallrye.reactive.messaging.cloudevents.CloudEventMessageBuilder;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class MyCloudEventProcessor {

    @Incoming("source")
    @Outgoing("result")
    public CloudEventMessage<String> process(CloudEventMessage<String> message) {
        return CloudEventMessageBuilder.from(message)
            .withData("Hello " + message.getPayload()).build();
    }
}
