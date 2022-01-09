package ack;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.mutiny.Uni;

public class AutoNack {

    // <auto-nack>
    @Incoming("data")
    @Outgoing("out")
    public String process(String s) {
        if (s.equalsIgnoreCase("b")) {
            // Throwing an exception triggers a nack
            throw new IllegalArgumentException("b");
        }

        if (s.equalsIgnoreCase("e")) {
            // Returning null would skip the message (it will be acked)
            return null;
        }

        return s.toUpperCase();
    }

    @Incoming("data")
    @Outgoing("out")
    public Uni<String> processAsync(String s) {
        if (s.equalsIgnoreCase("a")) {
            // Returning a failing Uni triggers a nack
            return Uni.createFrom().failure(new Exception("a"));
        }

        if (s.equalsIgnoreCase("b")) {
            // Throwing an exception triggers a nack
            throw new IllegalArgumentException("b");
        }

        if (s.equalsIgnoreCase("e")) {
            // Returning null would skip the message (it will be acked not nacked)
            return Uni.createFrom().nullItem();
        }

        if (s.equalsIgnoreCase("f")) {
            // returning `null` is invalid for method returning Unis, the message is nacked
            return null;
        }

        return Uni.createFrom().item(s.toUpperCase());
    }

    // </auto-nack>

}
