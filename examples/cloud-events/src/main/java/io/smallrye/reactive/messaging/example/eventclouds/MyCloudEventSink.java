package io.smallrye.reactive.messaging.example.eventclouds;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;

@ApplicationScoped
public class MyCloudEventSink {

    @Incoming("result")
    public void receive(String payload) {
        System.out.println("Received: " + payload);
    }
}
