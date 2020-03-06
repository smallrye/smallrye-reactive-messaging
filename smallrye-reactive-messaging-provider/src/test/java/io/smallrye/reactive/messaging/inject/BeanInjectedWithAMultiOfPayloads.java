package io.smallrye.reactive.messaging.inject;

import java.util.List;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;

import io.smallrye.mutiny.Multi;

@ApplicationScoped
public class BeanInjectedWithAMultiOfPayloads {

    private final Multi<String> constructor;
    @Inject
    @Channel("hello")
    private Multi<String> field;

    @Inject
    public BeanInjectedWithAMultiOfPayloads(@Channel("bonjour") Multi<String> constructor) {
        this.constructor = constructor;
    }

    public List<String> consume() {
        return Multi.createBy().concatenating()
                .streams(constructor, field)
                .collectItems().asList()
                .await().indefinitely();
    }

}
