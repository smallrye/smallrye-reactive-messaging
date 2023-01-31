package io.smallrye.reactive.messaging.beans;

import java.util.List;
import java.util.concurrent.*;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;

@ApplicationScoped
public class BeanConsumingPayloadsAndReturningACompletionStageOfSomething {

    private List<String> list = new CopyOnWriteArrayList<>();

    private ExecutorService executor = Executors.newSingleThreadExecutor();

    public void close() {
        executor.shutdown();
    }

    @Incoming("count")
    public CompletionStage<String> consume(String payload) {
        return CompletableFuture.supplyAsync(() -> {
            list.add(payload);
            return "hello";
        }, executor);
    }

    public List<String> payloads() {
        return list;
    }

}
