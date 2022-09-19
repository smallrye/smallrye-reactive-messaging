package io.smallrye.reactive.messaging.beans;

import java.util.ArrayList;
import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;

@ApplicationScoped
public class BeanConsumingPayloadsAndReturningSomething {

    private List<String> list = new ArrayList<>();

    @Incoming("count")
    public String consume(String payload) {
        list.add(payload);
        return payload;
    }

    public List<String> payloads() {
        return list;
    }
}
