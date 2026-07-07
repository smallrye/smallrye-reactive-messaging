package channelfactory;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.Cancellable;
import io.smallrye.reactive.messaging.ChannelFactory;
import io.smallrye.reactive.messaging.connector.IncomingConnectorConfiguration;
import io.smallrye.reactive.messaging.connector.OutgoingConnectorConfiguration;

@ApplicationScoped
public class ChannelBindingExample {

    @Inject
    ChannelFactory factory;

    // <subscribe>
    public Cancellable consumeMessages(IncomingConnectorConfiguration config) {
        return factory.incoming(config, String.class)
                .subscribe(payload -> {
                    // Process each message, auto-acknowledged on success
                    System.out.println("Received: " + payload);
                });
    }
    // </subscribe>

    // <subscribe-metadata>
    public Cancellable consumeWithMetadata(IncomingConnectorConfiguration config) {
        return factory.incoming(config, String.class)
                .subscribe((payload, metadata) -> {
                    // Access metadata alongside the payload
                    System.out.println("Received: " + payload + " with metadata: " + metadata);
                });
    }
    // </subscribe-metadata>

    // <subscribe-async>
    public Cancellable consumeAsync(IncomingConnectorConfiguration config) {
        return factory.incoming(config, String.class)
                .subscribe(payload -> {
                    // Async processing, ack on Uni completion
                    return Uni.createFrom().item(payload)
                            .onItem().invoke(p -> System.out.println("Processed: " + p))
                            .replaceWithVoid();
                });
    }
    // </subscribe-async>

    // <blocking>
    public Cancellable consumeBlocking(IncomingConnectorConfiguration config) {
        return factory.incoming(config, String.class)
                .blocking(4) // Process on worker threads, 4 messages concurrently
                .subscribe(payload -> {
                    // Blocking operation
                    System.out.println("Processed: " + payload);
                });
    }
    // </blocking>

    // <process>
    public Cancellable processAndForward(IncomingConnectorConfiguration inConfig,
            OutgoingConnectorConfiguration outConfig) {
        return factory.incoming(inConfig, String.class)
                .process(payload -> payload.toUpperCase())
                .to(outConfig);
    }
    // </process>
}
