package ack;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;

import javax.inject.Inject;
import java.util.concurrent.CompletionStage;

public class NackOnEmitter {

    // tag::emitter[]
    @Inject @Channel("data") Emitter<String> emitter;

    public void emitPayload() {
        CompletionStage<Void> completionStage = emitter.send("hello");
        completionStage.whenComplete((acked, nacked) -> {
            if (nacked != null) {
                // the processing has failed
            }
        });
    }
    // end::emitter[]

}
