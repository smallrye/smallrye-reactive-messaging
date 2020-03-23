package beans;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class PayloadProcessingBean {

    @Incoming("consumed-channel")
    @Outgoing("populated-channel")
    public String process(String in) {
        return in.toUpperCase();
    }
}
