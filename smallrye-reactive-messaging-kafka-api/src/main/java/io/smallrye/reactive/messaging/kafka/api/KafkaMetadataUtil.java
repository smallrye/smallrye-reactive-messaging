package io.smallrye.reactive.messaging.kafka.api;

import java.util.Optional;

import org.eclipse.microprofile.reactive.messaging.Message;

/**
 * Utility to access {@link KafkaMessageMetadata} in a {@link Message}.
 */
public class KafkaMetadataUtil {

    /**
     * Read {@link IncomingKafkaRecordMetadata} from a {@link Message}.
     *
     * @param msg the message. Must not be {@code null}
     * @return The {@link IncomingKafkaRecordMetadata}. May return {@code null}, for example if the message is
     *         not received from a channel backed by Kafka
     * @throws NullPointerException if {@code msg} is {@code null}
     */
    public static Optional<IncomingKafkaRecordMetadata> readIncomingKafkaMetadata(Message<?> msg) {
        return msg.getMetadata(IncomingKafkaRecordMetadata.class);
    }

    /**
     * Write {@link OutgoingKafkaRecordMetadata} to a {@link Message}. Note that {@code Message} is immutable,
     * so the passed in {@msg} parameter will not have the {@code OutgoingKafkaRecordMetadata} added. This
     * method returns a new instance of {@code Message} with the {@code OutgoingKafkaRecordMetadata} added.
     *
     * @param msg the message. Must not be {@code null}
     * @param outgoingKafkaRecordMetadata the {@link OutgoingKafkaRecordMetadata} to write. Must not be {@code null}
     * @return a clone of {@msg} with the {@link OutgoingKafkaRecordMetadata} added
     * @throws NullPointerException if {@code msg} or {@code outgoingKafkaRecordMetadata} are {@code null}
     */
    public static <T, K> Message<T> writeOutgoingKafkaMetadata(Message<T> msg,
            OutgoingKafkaRecordMetadata<K> outgoingKafkaRecordMetadata) {
        return msg.addMetadata(outgoingKafkaRecordMetadata);
    }
}
