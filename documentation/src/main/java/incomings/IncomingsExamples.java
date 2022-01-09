package incomings;

import org.eclipse.microprofile.reactive.messaging.Incoming;

public class IncomingsExamples {

    //<code>
    @Incoming("channel-1")
    @Incoming("channel-2")
    public String process(String s) {
        // get messages from channel-1 and channel-2
        return s.toUpperCase();
    }
    //</code>

}
