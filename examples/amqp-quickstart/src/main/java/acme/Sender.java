package acme;

import java.util.concurrent.*;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.reactive.messaging.amqp.AmqpMessage;

@ApplicationScoped
public class Sender {

    private ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    @Outgoing("data")
    public CompletionStage<AmqpMessage> send() {
        CompletableFuture<AmqpMessage> future = new CompletableFuture<>();
        delay(() -> {
            System.out.println("Sending timed message (outgoing-data)");
            future.complete(new AmqpMessage("hello from sender"));
        });
        return future;
    }

    private void delay(Runnable runnable) {
        executor.schedule(runnable, 5, TimeUnit.SECONDS);
    }

}
