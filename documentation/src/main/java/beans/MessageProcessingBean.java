package beans;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

@ApplicationScoped
public class MessageProcessingBean {

    @Incoming("consumed-channel")
    @Outgoing("populated-channel")
    public Message<String> process(Message<String> in) {
        // Process the payload
        String payload = in.getPayload().toUpperCase();
        // Create a new message from `in` and just update the payload
        return in.withPayload(payload);
    }
}
