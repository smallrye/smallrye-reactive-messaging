package camel.processor;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

@ApplicationScoped
public class CamelProcessor {

    @Incoming("mynatssubject")
    @Outgoing("mykafkatopic")
    public byte[] process(byte[] message) {
        // do some logic
        return message;
    }

}
