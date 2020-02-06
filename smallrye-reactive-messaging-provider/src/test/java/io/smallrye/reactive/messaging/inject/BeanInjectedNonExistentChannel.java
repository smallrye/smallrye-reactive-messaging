package io.smallrye.reactive.messaging.inject;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.reactivex.Flowable;

@ApplicationScoped
public class BeanInjectedNonExistentChannel {

    @Inject
    @Channel("idonotexist")
    private Flowable<Message<String>> field;

}
