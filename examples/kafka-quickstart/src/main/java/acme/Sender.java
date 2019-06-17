package acme;

import java.util.concurrent.*;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.reactive.messaging.kafka.KafkaMessage;

@ApplicationScoped
public class Sender {

    private ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    @Outgoing("data")
    public CompletionStage<KafkaMessage<String, String>> send() {
        CompletableFuture<KafkaMessage<String, String>> future = new CompletableFuture<>();
        delay(() -> future.complete(KafkaMessage.of("kafka", "key", "hello from MicroProfile")));
        return future;
    }

    private void delay(Runnable runnable) {
        executor.schedule(runnable, 5, TimeUnit.SECONDS);
    }

}
