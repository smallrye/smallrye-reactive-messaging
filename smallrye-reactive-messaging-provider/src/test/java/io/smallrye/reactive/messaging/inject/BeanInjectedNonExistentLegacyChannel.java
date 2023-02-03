package io.smallrye.reactive.messaging.inject;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.annotations.Channel;

@SuppressWarnings("deprecation")
@ApplicationScoped
public class BeanInjectedNonExistentLegacyChannel {

    @Inject
    @Channel("idonotexist")
    private Multi<Message<String>> field;

    public void goingToFail() {
        field.collect().asList().await().indefinitely();
    }

}
