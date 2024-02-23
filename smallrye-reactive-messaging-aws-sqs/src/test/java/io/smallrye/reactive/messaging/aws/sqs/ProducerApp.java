package io.smallrye.reactive.messaging.aws.sqs;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.Dependent;
import java.util.concurrent.atomic.AtomicInteger;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

@Dependent
public class ProducerApp {

    private static final AtomicInteger COUNTER = new AtomicInteger(0);

    @Outgoing("data")
    public Uni<String> send() {
        if (COUNTER.getAndIncrement() > 9) {
            return Uni.createFrom().nullItem();
        }
        return Uni.createFrom().item(String.format("hello-%d", COUNTER.get()));
    }
}
