package io.smallrye.reactive.messaging.inject;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.Multi;

@ApplicationScoped
public class BeanInjectedNonExistentChannel {

    @Inject
    @Channel("idonotexist")
    private Multi<Message<String>> field;

}
