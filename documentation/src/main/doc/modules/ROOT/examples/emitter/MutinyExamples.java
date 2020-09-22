package emitter;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.Cancellable;
import io.smallrye.reactive.messaging.MutinyEmitter;
import org.eclipse.microprofile.reactive.messaging.Channel;

import javax.inject.Inject;

public class MutinyExamples {
    // tag::uni[]
    @Inject
    @Channel("prices")
    MutinyEmitter<Double> emitter;

    public Uni<Void> send(double d) {
        return emitter.send(d);
    }
    // end::uni[]

    // tag::uni-await[]
    public void sendAwait(double d) {
        emitter.sendAndAwait(d);
    }
    // end::uni-await[]

    // tag::uni-forget[]
    public Cancellable sendForget(double d) {
        return emitter.sendAndForget(d);
    }
    // end::uni-forget[]
}
