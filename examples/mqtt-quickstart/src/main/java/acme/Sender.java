package acme;

import java.util.concurrent.*;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.reactive.messaging.mqtt.MqttMessage;

@ApplicationScoped
public class Sender {

    private ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    @Outgoing("data")
    public CompletionStage<MqttMessage> send() {
        CompletableFuture<MqttMessage> future = new CompletableFuture<>();
        delay(() -> {
            System.out.println("Sending message on topic: hello");
            future.complete(MqttMessage.of("hello", "hello from dynamic topic",
                    null, true));
        });
        return future;
    }

    private void delay(Runnable runnable) {
        executor.schedule(runnable, 5, TimeUnit.SECONDS);
    }

}
