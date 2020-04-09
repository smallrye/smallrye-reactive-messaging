package acme;

import io.smallrye.reactive.messaging.pulsar.PulsarMessage;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;

import javax.enterprise.inject.se.SeContainer;
import javax.enterprise.inject.se.SeContainerInitializer;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {


    public static void main(String[] args) {
        SeContainer container = SeContainerInitializer.newInstance().initialize();
    }

    @Incoming("producer")
    public void consume(byte [] message) {
        System.out.println("received: " + String.valueOf(message));
    }


    @Outgoing("consumer")
    public String send() {
        return "Message";
    }
}
