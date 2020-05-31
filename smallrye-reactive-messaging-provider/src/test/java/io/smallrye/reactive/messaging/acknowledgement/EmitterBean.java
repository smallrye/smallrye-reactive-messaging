package io.smallrye.reactive.messaging.acknowledgement;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;

@ApplicationScoped
public class EmitterBean {
    @Inject
    @Channel("data")
    Emitter<String> emitter;

    public Emitter<String> emitter() {
        return emitter;
    }
}
