package io.smallrye.reactive.messaging.providers.locals;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.PublisherDecorator;

/**
 * Decorator to dispatch messages on the Vert.x context attached to the message via {@link LocalContextMetadata}.
 * Low priority to be called before other decorators.
 */
@ApplicationScoped
public class ContextDecorator implements PublisherDecorator {

    @Override
    public int getPriority() {
        return 0;
    }

    @Override
    public Multi<? extends Message<?>> decorate(Multi<? extends Message<?>> publisher, String channelName,
            boolean isConnector) {
        return ContextOperator.apply(publisher);
    }

}
