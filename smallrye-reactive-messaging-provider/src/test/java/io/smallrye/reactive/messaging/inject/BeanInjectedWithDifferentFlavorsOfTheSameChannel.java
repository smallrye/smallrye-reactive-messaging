package io.smallrye.reactive.messaging.inject;

import java.util.LinkedHashMap;
import java.util.Map;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.reactivestreams.Publisher;

import io.smallrye.mutiny.Multi;

@ApplicationScoped
public class BeanInjectedWithDifferentFlavorsOfTheSameChannel {

    @Inject
    @Channel("hello")
    private Multi<Message<String>> field1;

    @Inject
    @Channel("hello")
    private Multi<Message<String>> field2;

    @Inject
    @Channel("hello")
    private Publisher<Message<String>> field3;

    @Inject
    @Channel("hello")
    private Publisher<Message> field4;

    @Inject
    @Channel("hello")
    private Multi<Message> field5;

    @Inject
    @Channel("hello")
    private PublisherBuilder<Message> field6;

    @Inject
    @Channel("hello")
    private PublisherBuilder<Message<String>> field7;

    @Inject
    @Channel("hello")
    private PublisherBuilder<String> field8;

    @Inject
    @Channel("hello")
    private Publisher<String> field9;

    @Inject
    @Channel("hello")
    private Multi<String> field10;

    public Map<String, String> consume() {
        Map<String, String> map = new LinkedHashMap<>();
        map.put("1", field1.toString());
        map.put("2", field2.toString());
        map.put("3", field3.toString());
        map.put("4", field4.toString());
        map.put("5", field5.toString());
        map.put("6", field6.toString());
        map.put("7", field7.toString());
        map.put("8", field8.toString());
        map.put("9", field9.toString());
        map.put("10", field10.toString());
        return map;
    }

}
