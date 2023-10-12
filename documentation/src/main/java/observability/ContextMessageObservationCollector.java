package observability;

import java.time.Duration;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.reactive.messaging.observation.DefaultMessageObservation;
import io.smallrye.reactive.messaging.observation.MessageObservation;
import io.smallrye.reactive.messaging.observation.MessageObservationCollector;
import io.smallrye.reactive.messaging.observation.ObservationContext;

@ApplicationScoped
public class ContextMessageObservationCollector
        implements MessageObservationCollector<ContextMessageObservationCollector.MyContext> {

    @Override
    public MyContext initObservation(String channel, boolean incoming, boolean emitter) {
        // Called on observation setup, per channel
        // if returned null the observation for that channel is disabled
        return new MyContext(channel, incoming, emitter);
    }

    @Override
    public MessageObservation onNewMessage(String channel, Message<?> message, MyContext ctx) {
        // Called after message has been created
        return new DefaultMessageObservation(channel);
    }

    public static class MyContext implements ObservationContext {

        private final String channel;
        private final boolean incoming;
        private final boolean emitter;

        public MyContext(String channel, boolean incoming, boolean emitter) {
            this.channel = channel;
            this.incoming = incoming;
            this.emitter = emitter;
        }

        @Override
        public void complete(MessageObservation observation) {
            // called after message processing has completed and observation is done
            // register duration
            Duration duration = observation.getCompletionDuration();
            Throwable reason = observation.getReason();
            if (reason != null) {
                // message was nacked
            } else {
                // message was acked successfully
            }
        }
    }

}
