package io.smallrye.reactive.messaging.rabbitmq;

import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.reactive.messaging.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A bean that can be registered to test rejecting and requeuing the
 * first delivery attempt. Redeliveries will be nack'ed without requeue,
 * so they should end up in the DLQ.
 */
@ApplicationScoped
public class RequeueFirstDeliveryBean {
    private final List<Integer> list = new ArrayList<>();
    private final List<Integer> dlqList = new ArrayList<>();

    private final AtomicInteger typeCastCounter = new AtomicInteger();

    @Incoming("data")
    @Outgoing("sink")
    @Acknowledgment(Acknowledgment.Strategy.MANUAL)
    public Message<Integer> process(Message<String> input) {
        int value = -1;
        try {
            value = Integer.parseInt(input.getPayload());
        } catch (ClassCastException e) {
            typeCastCounter.incrementAndGet();
        }

        return Message.of(value + 1, () -> {
            boolean isRedeliver = input.getMetadata(IncomingRabbitMQMetadata.class)
                    .map(IncomingRabbitMQMetadata::isRedeliver)
                    .orElse(false);

            if (isRedeliver) {
                return input.nack(new RuntimeException("reject"));
            } else {
                return input.nack(new RuntimeException("requeue"), Metadata.of(new RabbitMQRejectMetadata(true)));
            }
        });
    }

    @Incoming("sink")
    public void sink(int val) {
        list.add(val);
    }

    @Incoming("data-dlq")
    @Acknowledgment(Acknowledgment.Strategy.MANUAL)
    public CompletionStage<Void> dlq(Message<String> msg) {
        try {
            dlqList.add(Integer.parseInt(msg.getPayload()));
        } catch (ClassCastException cce) {
            typeCastCounter.incrementAndGet();
        }

        return msg.ack();
    }

    public List<Integer> getResults() {
        return list;
    }

    public List<Integer> getDlqResults() {
        return dlqList;
    }

    public int getTypeCasts() {
        return typeCastCounter.get();
    }
}
