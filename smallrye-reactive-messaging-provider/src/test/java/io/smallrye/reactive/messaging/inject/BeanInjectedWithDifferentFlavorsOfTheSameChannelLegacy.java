package io.smallrye.reactive.messaging.inject;

import java.util.LinkedHashMap;
import java.util.Map;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.reactivestreams.Publisher;

import io.smallrye.reactive.messaging.annotations.Channel;

@SuppressWarnings("deprecation")
@ApplicationScoped
public class BeanInjectedWithDifferentFlavorsOfTheSameChannelLegacy {

    @Inject
    @Channel("hello")
    private Publisher<Message<String>> field3;

    @SuppressWarnings("rawtypes")
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

    public Map<String, String> consume() {
        Map<String, String> map = new LinkedHashMap<>();
        map.put("3", field3.toString());
        map.put("6", field6.toString());
        map.put("7", field7.toString());
        map.put("8", field8.toString());
        map.put("9", field9.toString());
        return map;
    }

}
