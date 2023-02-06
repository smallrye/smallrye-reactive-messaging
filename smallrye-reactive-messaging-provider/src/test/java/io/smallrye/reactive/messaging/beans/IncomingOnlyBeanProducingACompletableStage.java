package io.smallrye.reactive.messaging.beans;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

@ApplicationScoped
public class IncomingOnlyBeanProducingACompletableStage {

    private List<Integer> list = new ArrayList<>();

    @Incoming("count")
    public CompletionStage<Void> process(Message<String> value) {
        list.add(Integer.valueOf(value.getPayload()));
        return CompletableFuture.completedFuture(null);
    }

    public List<Integer> list() {
        return list;
    }

}
