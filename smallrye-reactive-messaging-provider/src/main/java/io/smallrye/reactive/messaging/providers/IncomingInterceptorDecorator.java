package io.smallrye.reactive.messaging.providers;

import static io.smallrye.reactive.messaging.providers.helpers.CDIUtils.getInstanceById;
import static io.smallrye.reactive.messaging.providers.helpers.CDIUtils.getSortedInstances;

import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Default;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.unchecked.Unchecked;
import io.smallrye.reactive.messaging.IncomingInterceptor;
import io.smallrye.reactive.messaging.PublisherDecorator;

/**
 * Decorator to support {@link IncomingInterceptor}s.
 * High priority value to be called after other decorators.
 */
@ApplicationScoped
public class IncomingInterceptorDecorator implements PublisherDecorator {

    @Any
    @Inject
    Instance<IncomingInterceptor> interceptors;

    @Override
    public int getPriority() {
        return 500;
    }

    @Override
    public Multi<? extends Message<?>> decorate(Multi<? extends Message<?>> publisher, String channelName,
            boolean isConnector) {
        Multi<? extends Message<?>> multi = publisher;
        if (isConnector) {
            Instance<IncomingInterceptor> instances = getInstanceById(interceptors, channelName);
            if (instances.isUnsatisfied()) {
                instances = interceptors.select().select(Default.Literal.INSTANCE);
            }
            List<IncomingInterceptor> matching = getSortedInstances(instances);
            if (!matching.isEmpty()) {
                IncomingInterceptor interceptor = matching.get(0);
                multi = multi.map(m -> {
                    Message<?> before = interceptor.onMessage(m);
                    Message<?> withAck = before.withAck(() -> before.ack()
                            .thenAccept(Unchecked.consumer(x -> interceptor.onMessageAck(before))));
                    return withAck.withNack(t -> withAck.nack(t)
                            .thenAccept(Unchecked.consumer(x -> interceptor.onMessageNack(withAck, t))));
                });
            }
        }
        return multi;
    }

}
