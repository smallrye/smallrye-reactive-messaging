package io.smallrye.reactive.messaging.beans;

import java.util.List;
import java.util.concurrent.*;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;

@ApplicationScoped
public class BeanConsumingPayloadsAndReturningACompletionStageOfVoid {

    private List<String> list = new CopyOnWriteArrayList<>();

    private ExecutorService executor = Executors.newSingleThreadExecutor();

    @Incoming("count")
    public CompletionStage<Void> consume(String payload) {
        return CompletableFuture.supplyAsync(() -> {
            list.add(payload);
            return null;
        }, executor);
    }

    public void close() {
        executor.shutdown();
    }

    public List<String> payloads() {
        return list;
    }

}
