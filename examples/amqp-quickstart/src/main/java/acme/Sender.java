package acme;

import java.util.concurrent.*;

import javax.enterprise.context.ApplicationScoped;

import io.smallrye.reactive.messaging.amqp.AmqpMessageHelper;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

@ApplicationScoped
public class Sender {

    private ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    @Outgoing("data")
    public CompletionStage<Message> send() {
        CompletableFuture<Message> future = new CompletableFuture<>();
        delay(() -> {
            String message = "hello from sender";
            System.out.println("Sending (data): " + message);
            future.complete(Message.newBuilder().payload(message).build());
        });
        return future;
    }

    private void delay(Runnable runnable) {
        executor.schedule(runnable, 5, TimeUnit.SECONDS);
    }

}
