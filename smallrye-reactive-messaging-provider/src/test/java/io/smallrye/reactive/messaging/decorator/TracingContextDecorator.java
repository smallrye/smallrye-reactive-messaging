package io.smallrye.reactive.messaging.decorator;

import java.util.List;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.SubscriberDecorator;
import io.smallrye.reactive.messaging.providers.locals.LocalContextMetadata;
import io.vertx.core.Context;

@ApplicationScoped
public class TracingContextDecorator implements SubscriberDecorator {

    public static Trace trace = new Trace();

    @Override
    public Multi<? extends Message<?>> decorate(Multi<? extends Message<?>> toBeSubscribed,
            List<String> channelName, boolean isConnector) {
        Multi<? extends Message<?>> multi = toBeSubscribed;
        multi = multi.map(m -> {
            Optional<LocalContextMetadata> metadata = m.getMetadata(LocalContextMetadata.class);
            assert metadata.isPresent();
            if (metadata.isPresent()) {
                LocalContextMetadata contextMetadata = metadata.get();
                Context context = contextMetadata.context();
                assert context != null;
            }
            return m.addMetadata(trace);
        });
        return multi;
    }

    public static class Trace {

    }
}
