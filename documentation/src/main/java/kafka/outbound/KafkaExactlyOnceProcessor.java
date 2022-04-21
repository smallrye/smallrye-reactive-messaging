package kafka.outbound;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Incoming;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import io.smallrye.reactive.messaging.kafka.KafkaRecordBatch;
import io.smallrye.reactive.messaging.kafka.transactions.KafkaTransactions;

@ApplicationScoped
public class KafkaExactlyOnceProcessor {

    @Inject
    @Channel("tx-out-example")
    KafkaTransactions<Integer> txProducer;

    @Incoming("in-channel")
    public Uni<Void> emitInTransaction(KafkaRecordBatch<String, Integer> batch) {
        return txProducer.withTransaction(batch, emitter -> {
            for (KafkaRecord<String, Integer> record : batch) {
                emitter.send(KafkaRecord.of(record.getKey(), record.getPayload() + 1));
            }
            return Uni.createFrom().voidItem();
        });
    }

}
