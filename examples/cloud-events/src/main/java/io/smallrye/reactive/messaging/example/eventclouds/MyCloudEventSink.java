package io.smallrye.reactive.messaging.example.eventclouds;

import org.eclipse.microprofile.reactive.messaging.Incoming;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class MyCloudEventSink {

    @Incoming("result")
    public void receive(String payload) {
        System.out.println("Received: " + payload);
    }
}
