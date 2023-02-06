package io.smallrye.reactive.messaging.beans;

import java.util.List;
import java.util.concurrent.*;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

@ApplicationScoped
public class BeanConsumingMessagesAndReturningACompletionStageOfSomething {

    private List<String> list = new CopyOnWriteArrayList<>();

    private ExecutorService executor = Executors.newSingleThreadExecutor();

    @Incoming("count")
    CompletionStage<String> consume(Message<String> msg) {
        return CompletableFuture.supplyAsync(() -> {
            list.add(msg.getPayload());
            return "hello";
        }, executor);
    }

    public void close() {
        executor.shutdown();
    }

    public List<String> payloads() {
        return list;
    }

}
