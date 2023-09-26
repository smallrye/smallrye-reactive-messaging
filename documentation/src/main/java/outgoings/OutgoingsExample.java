package outgoings;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

public class OutgoingsExample {

    //<code>
    @Incoming("in")
    @Outgoing("out1")
    @Outgoing("out2")
    public String process(String s) {
        // send messages from channel-in to both channel-out1 and channel-out2
        return s.toUpperCase();
    }
    //</code>

}
