package acme;

import java.util.concurrent.*;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.reactive.messaging.kafka.KafkaRecord;

@ApplicationScoped
public class Sender {

    private ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    @Outgoing("data")
    public CompletionStage<KafkaRecord<String, String>> send() {
        CompletableFuture<KafkaRecord<String, String>> future = new CompletableFuture<>();
        delay(() -> future.complete(KafkaRecord.of("kafka", "key", "hello from MicroProfile")));
        return future;
    }

    private void delay(Runnable runnable) {
        executor.schedule(runnable, 5, TimeUnit.SECONDS);
    }

}
