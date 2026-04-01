package kafka.outbound;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Incoming;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.annotations.Blocking;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.transactions.KafkaTransactions;

@ApplicationScoped
public class KafkaPooledExactlyOnceProcessor {

    @Inject
    @Channel("tx-out-example")
    KafkaTransactions<Integer> txProducer;

    @Incoming("in-channel")
    @Blocking(ordered = false)
    public void process(ConsumerRecord<String, Integer> record, IncomingKafkaRecordMetadata<String, Integer> metadata) {
        txProducer.withTransactionAndAwait(metadata, emitter -> {
            emitter.send(KafkaRecord.of(record.key(), record.value() + 1));
            return Uni.createFrom().voidItem();
        });
    }

}
