package emitter;

import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.Cancellable;
import io.smallrye.reactive.messaging.MutinyEmitter;

public class MutinyExamples {
    // <uni>
    @Inject
    @Channel("prices")
    MutinyEmitter<Double> emitter;

    public Uni<Void> send(double d) {
        return emitter.send(d);
    }
    // </uni>

    // <uni-await>
    public void sendAwait(double d) {
        emitter.sendAndAwait(d);
    }
    // </uni-await>

    // <uni-forget>
    public Cancellable sendForget(double d) {
        return emitter.sendAndForget(d);
    }
    // </uni-forget>
}
