package channelfactory;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.smallrye.reactive.messaging.ChannelFactory;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.connector.OutgoingConnectorConfiguration;

// <outgoing>
@ApplicationScoped
public class ChannelFactoryOutgoingExample {

    @Inject
    ChannelFactory factory;

    public void createOutgoingChannel(OutgoingConnectorConfiguration config) {
        // Create an outgoing channel with an Emitter from a connector configuration
        MutinyEmitter<String> emitter = factory.outgoing(config, MutinyEmitter.class);

        // Send messages programmatically
        emitter.send("hello");
        emitter.send("world");
    }
}
// </outgoing>
