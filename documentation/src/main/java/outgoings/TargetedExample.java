package outgoings;

import java.util.Map;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.reactive.messaging.Messages;
import io.smallrye.reactive.messaging.Targeted;
import io.smallrye.reactive.messaging.TargetedMessages;

public class TargetedExample {

    //<code>
    @Incoming("in")
    @Outgoing("out1")
    @Outgoing("out2")
    @Outgoing("out3")
    public Targeted process(double price) {
        // send messages from channel-in to both channel-out1 and channel-out2
        Targeted targeted = Targeted.of("out1", "Price: " + price,
                "out2", "Quote: " + price);
        if (price > 90.0) {
            return targeted.with("out3", price);
        }
        return targeted;
    }
    //</code>

    //<targeted-messages>
    @Incoming("channel-in")
    @Outgoing("channel-out1")
    @Outgoing("channel-out2")
    @Outgoing("channel-out3")
    public TargetedMessages processMessage(Message<String> msg) {
        // send messages from channel-in to both channel-out1 and channel-out2
        return Messages.chain(msg)
                .with(Map.of("channel-out1", msg.withPayload(msg.getPayload().toUpperCase()),
                        "channel-out2", msg.withPayload(msg.getPayload().toLowerCase())));
    }
    //</targeted-messages>

}
