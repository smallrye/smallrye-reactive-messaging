package ack;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

public class PreAckExamples {

    // <pre>
    @Incoming("in")
    @Outgoing("out")
    @Acknowledgment(Acknowledgment.Strategy.PRE_PROCESSING)
    public String process(String input) {
        // The message wrapping the payload is already acknowledged
        // The default would have waited the produced message to be
        // acknowledged
        return input.toUpperCase();
    }
    // </pre>
}
