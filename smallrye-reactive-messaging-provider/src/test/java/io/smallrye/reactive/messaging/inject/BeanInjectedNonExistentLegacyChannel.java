package io.smallrye.reactive.messaging.inject;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.reactivex.Flowable;
import io.smallrye.reactive.messaging.annotations.Channel;

@ApplicationScoped
public class BeanInjectedNonExistentLegacyChannel {

    @Inject
    @Channel("idonotexist")
    private Flowable<Message<String>> field;

}
