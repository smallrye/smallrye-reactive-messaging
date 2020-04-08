package io.smallrye.reactive.messaging.example.eventclouds;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.reactive.messaging.cloudevents.CloudEventMessage;
import io.smallrye.reactive.messaging.cloudevents.CloudEventMessageBuilder;

@ApplicationScoped
public class MyCloudEventProcessor {

    @Incoming("source")
    @Outgoing("result")
    public CloudEventMessage<String> process(CloudEventMessage<String> message) {
        return CloudEventMessageBuilder.from(message)
                .withData("Hello " + message.getPayload()).build();
    }
}
