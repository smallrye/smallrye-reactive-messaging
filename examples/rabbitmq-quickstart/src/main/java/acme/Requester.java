package acme;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;

import io.smallrye.reactive.messaging.rabbitmq.reply.RabbitMQRequestReply;

@ApplicationScoped
public class Requester {

    @Inject
    @Channel("request-reply")
    RabbitMQRequestReply<String, String> requestReply;

    public void periodicallyRequest() {
        AtomicInteger counter = new AtomicInteger();
        Executors.newSingleThreadScheduledExecutor()
                .scheduleAtFixedRate(() -> {
                    String request = "Request " + counter.getAndIncrement();
                    System.out.println("Sending request: " + request);
                    requestReply.request(request)
                            .subscribe().with(reply -> System.out.println("Received reply: " + reply),
                                    failure -> System.err.println("Request failed: " + failure.getMessage()));
                },
                        1, 2, TimeUnit.SECONDS);
    }

}
