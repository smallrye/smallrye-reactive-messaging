package io.smallrye.reactive.messaging.rabbitmq;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.*;

/**
 * A bean that can be registered to test rejecting and requeuing the
 * first delivery attempt. Redeliveries will be nack'ed without requeue,
 * so they should end up in the DLQ.
 */
@ApplicationScoped
public class RequeueFirstDeliveryBean {
    private final List<Integer> list = new CopyOnWriteArrayList<>();
    private final List<Integer> redelivered = new CopyOnWriteArrayList<>();
    private final List<Integer> dlqList = new CopyOnWriteArrayList<>();

    @Incoming("data")
    public CompletionStage<Void> process(Message<String> input) {
        int value = Integer.parseInt(input.getPayload());
        list.add(value + 1);

        boolean redeliver = input.getMetadata(IncomingRabbitMQMetadata.class)
                .map(IncomingRabbitMQMetadata::isRedeliver)
                .orElse(false);
        if (redeliver) {
            redelivered.add(value + 1);
        }
        return input.nack(new RuntimeException("requeue"), Metadata.of(new RabbitMQRejectMetadata(true)));
    }

    @Incoming("data-dlq")
    public void dlq(String msg) {
        dlqList.add(Integer.parseInt(msg));
    }

    public List<Integer> getResults() {
        return list;
    }

    public List<Integer> getDlqResults() {
        return dlqList;
    }

    public List<Integer> getRedelivered() {
        return redelivered;
    }
}
