package io.smallrye.reactive.messaging.inject;

import java.util.Map;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;

import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.Table;

@ApplicationScoped
public class BeanInjectedWithATable {

    @Inject
    @Channel("hello")
    private MutinyEmitter<String> helloEmitter;

    @Inject
    @Channel("hello")
    private Table<String, String> hello;

    @Inject
    @Channel("bonjour")
    private Table<String, String> bonjour;

    public Map<String, String> getMapBonjour() {
        return bonjour.toMap();
    }

    public Table<String, String> getHello() {
        return hello;
    }

    public MutinyEmitter<String> getHelloEmitter() {
        return helloEmitter;
    }

    public Table<String, String> getBonjour() {
        return bonjour;
    }
}
