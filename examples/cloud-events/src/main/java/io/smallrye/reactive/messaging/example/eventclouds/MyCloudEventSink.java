package io.smallrye.reactive.messaging.example.eventclouds;

import org.eclipse.microprofile.reactive.messaging.Incoming;

import javax.enterprise.context.ApplicationScoped;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@ApplicationScoped
public class MyCloudEventSink {

  @Incoming("result")
  public CompletionStage<Void> receive(String payload) {
    System.out.println("Received: " + payload);
    return CompletableFuture.completedFuture(null);
  }
}
