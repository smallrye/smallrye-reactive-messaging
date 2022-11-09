package beans;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

@ApplicationScoped
public class PayloadProcessingBean {

    @Incoming("consumed-channel")
    @Outgoing("populated-channel")
    public String process(String in) {
        return in.toUpperCase();
    }
}
