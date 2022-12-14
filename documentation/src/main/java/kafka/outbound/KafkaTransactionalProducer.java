package kafka.outbound;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import io.smallrye.reactive.messaging.kafka.transactions.KafkaTransactions;

@ApplicationScoped
public class KafkaTransactionalProducer {

    @Inject
    @Channel("tx-out-example")
    KafkaTransactions<String> txProducer;

    public Uni<Void> emitInTransaction() {
        return txProducer.withTransaction(emitter -> {
            emitter.send(KafkaRecord.of(1, "a"));
            emitter.send(KafkaRecord.of(2, "b"));
            emitter.send(KafkaRecord.of(3, "c"));
            return Uni.createFrom().voidItem();
        });
    }

}
