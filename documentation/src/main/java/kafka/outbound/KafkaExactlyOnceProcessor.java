package kafka.outbound;

import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import io.smallrye.reactive.messaging.kafka.Record;
import io.smallrye.reactive.messaging.kafka.transactions.KafkaTransactions;

@ApplicationScoped
public class KafkaExactlyOnceProcessor {

    @Inject
    @Channel("tx-out-example")
    KafkaTransactions<Integer> txProducer;

    @Incoming("in-channel")
    public Uni<Void> emitInTransaction(Message<List<Record<String, Integer>>> batch) {
        return txProducer.withTransaction(batch, emitter -> {
            for (Record<String, Integer> record : batch.getPayload()) {
                emitter.send(KafkaRecord.of(record.key(), record.value() + 1));
            }
            return Uni.createFrom().voidItem();
        });
    }

}
