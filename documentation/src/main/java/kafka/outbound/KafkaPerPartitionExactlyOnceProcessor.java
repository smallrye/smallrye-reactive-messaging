package kafka.outbound;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Incoming;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import io.smallrye.reactive.messaging.kafka.transactions.KafkaTransactions;

@ApplicationScoped
public class KafkaPerPartitionExactlyOnceProcessor {

    @Inject
    @Channel("tx-out-example")
    KafkaTransactions<Integer> txProducer;

    @Incoming("in-channel")
    public Uni<Void> process(KafkaRecord<String, Integer> record) {
        return txProducer.withTransaction(record, emitter -> {
            // Outgoing partition defaults to the incoming record's partition
            emitter.send(KafkaRecord.of(record.getKey(), record.getPayload() + 1));
            return Uni.createFrom().voidItem();
        });
    }

}
