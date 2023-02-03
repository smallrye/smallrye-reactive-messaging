package io.smallrye.reactive.messaging.inject;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.Multi;

@ApplicationScoped
public class BeanInjectedNonExistentChannel {

    @Inject
    @Channel("idonotexist")
    private Multi<Message<String>> field;

    public Multi<Message<String>> getChannel() {
        return field;
    }

}
