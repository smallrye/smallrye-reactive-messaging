package acme;

import java.util.concurrent.*;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.reactive.messaging.amqp.AmqpMessage;

@ApplicationScoped
public class Sender {

    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    @Outgoing("data")
    public CompletionStage<AmqpMessage> send() {
        CompletableFuture<AmqpMessage> future = new CompletableFuture<>();
        delay(() -> {
            String message = "hello from sender";
            System.out.println("Sending (data): " + message);
            future.complete(AmqpMessage.builder().withBody(message).build());
        });
        return future;
    }

    private void delay(Runnable runnable) {
        executor.schedule(runnable, 5, TimeUnit.SECONDS);
    }

}
