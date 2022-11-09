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
import io.smallrye.reactive.messaging.OutgoingInterceptor;
import io.smallrye.reactive.messaging.OutgoingMessageMetadata;
import io.smallrye.reactive.messaging.SubscriberDecorator;

/**
 * Decorator to support {@link OutgoingInterceptor}s.
 * High priority value to be called after other decorators.
 */
@ApplicationScoped
public class OutgoingInterceptorDecorator implements SubscriberDecorator {

    @Any
    @Inject
    Instance<OutgoingInterceptor> interceptors;

    @Override
    public int getPriority() {
        return 2000;
    }

    @Override
    public Multi<? extends Message<?>> decorate(Multi<? extends Message<?>> toBeSubscribed, List<String> channelName,
            boolean isConnector) {
        Multi<? extends Message<?>> multi = toBeSubscribed;
        if (isConnector) {
            Instance<OutgoingInterceptor> instances = getInstanceById(interceptors, channelName.get(0));
            if (instances.isUnsatisfied()) {
                instances = interceptors.select().select(Default.Literal.INSTANCE);
            }
            List<OutgoingInterceptor> matching = getSortedInstances(instances);
            if (!matching.isEmpty()) {
                OutgoingInterceptor interceptor = matching.get(0);
                multi = multi.map(m -> {
                    Message<?> before = interceptor.onMessage(m.addMetadata(new OutgoingMessageMetadata<>()));
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
