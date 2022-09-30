package ack;

import java.util.concurrent.CompletionStage;

import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;

public class NackOnEmitter {

    // <emitter>
    @Inject
    @Channel("data")
    Emitter<String> emitter;

    public void emitPayload() {
        CompletionStage<Void> completionStage = emitter.send("hello");
        completionStage.whenComplete((acked, nacked) -> {
            if (nacked != null) {
                // the processing has failed
            }
        });
    }
    // </emitter>

}
