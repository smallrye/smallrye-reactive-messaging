package observability;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.reactive.messaging.observation.DefaultMessageObservation;
import io.smallrye.reactive.messaging.observation.MessageObservation;
import io.smallrye.reactive.messaging.observation.MessageObservationCollector;
import io.smallrye.reactive.messaging.observation.ObservationContext;

@ApplicationScoped
public class SimpleMessageObservationCollector implements MessageObservationCollector<ObservationContext> {

    @Override
    public MessageObservation onNewMessage(String channel, Message<?> message, ObservationContext ctx) {
        // Called after message has been created
        return new DefaultMessageObservation(channel);
    }

}
