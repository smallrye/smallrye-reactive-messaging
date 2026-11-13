package channelfactory;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.smallrye.reactive.messaging.ChannelFactory;
import io.smallrye.reactive.messaging.connector.IncomingConnectorConfiguration;

// <incoming>
@ApplicationScoped
public class ChannelFactoryIncomingExample {

    @Inject
    ChannelFactory factory;

    public void createIncomingChannel(IncomingConnectorConfiguration config) {
        // Create an incoming channel from a connector configuration and subscribe
        factory.incoming(config, String.class)
                .subscribe(payload -> {
                    // Process each message
                });
    }
}
// </incoming>
