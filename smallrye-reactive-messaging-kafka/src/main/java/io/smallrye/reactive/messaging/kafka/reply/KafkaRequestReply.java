package io.smallrye.reactive.messaging.kafka.reply;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.TopicPartition;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.common.annotation.Experimental;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.EmitterType;
import io.smallrye.reactive.messaging.kafka.KafkaConsumer;

/**
 * KafkaRequestReply is an experimental API that provides functionality for sending requests and receiving
 * responses over Kafka.
 *
 * @param <Req> the type of the request value
 * @param <Rep> the type of the response value
 */
@Experimental("Experimental API")
public interface KafkaRequestReply<Req, Rep> extends EmitterType {

    /**
     * The default header key used for correlating replies to requests.
     */
    String DEFAULT_REPLY_CORRELATION_ID_HEADER = "REPLY_CORRELATION_ID";

    /**
     * The config key for specifying the header key used to store the correlation ID in the reply record header.
     */
    String REPLY_CORRELATION_ID_HEADER_KEY = "reply.correlation-id.header";

    /**
     * The default header key used for indicating the topic of the reply record.
     */
    String DEFAULT_REPLY_TOPIC_HEADER = "REPLY_TOPIC";

    /**
     * The config key for specifying the header key used to store the reply topic in the reply record header.
     */
    String REPLY_TOPIC_HEADER_KEY = "reply.topic.header";

    /**
     * The default header key used for indicating the partition of the reply record.
     */
    String DEFAULT_REPLY_PARTITION_HEADER = "REPLY_PARTITION";

    /**
     * The config key for specifying the header key used to store the reply partition in the reply record header.
     */
    String REPLY_PARTITION_HEADER_KEY = "reply.partition.header";

    /**
     * The default suffix used to create a separate topic for sending reply messages.
     */
    String DEFAULT_REPLIES_TOPIC_SUFFIX = "-replies";

    /**
     * The config key for the reply topic.
     */
    String REPLY_TOPIC_KEY = "reply.topic";

    /**
     * The config key for the reply partition.
     */
    String REPLY_PARTITION_KEY = "reply.partition";

    /**
     * The config key for the reply timeout.
     */
    String REPLY_TIMEOUT_KEY = "reply.timeout";

    /**
     * The config key for the correlation ID handler identifier.
     * <p>
     * This config is used to select a CDI-managed implementation of {@link CorrelationIdHandler}
     * identified with this config.
     * <p>
     * The correlation ID handler is responsible for generating and handling the correlation ID when replying to a request.
     *
     * @see CorrelationIdHandler
     */
    String REPLY_CORRELATION_ID_HANDLER_KEY = "reply.correlation-id.handler";

    /**
     * The default correlation ID handler identifier.
     * <p>
     * The "uuid" correlation ID handler generates unique correlation
     * IDs using universally unique identifiers (UUIDs).
     */
    String DEFAULT_CORRELATION_ID_HANDLER = "uuid";

    /**
     * The config key for the reply failure handler identifier.
     * <p>
     * This config is used to select a CDI-managed implementation of {@link ReplyFailureHandler}
     * <p>
     * The Reply Failure Handler is responsible for extracting failure from the reply record.
     *
     * @see ReplyFailureHandler
     */
    String REPLY_FAILURE_HANDLER_KEY = "reply.failure.handler";

    /**
     * Sends a request and receives a response.
     *
     * @param request the request object to be sent
     * @return a Uni object representing the result of the send and receive operation
     */
    Uni<Rep> request(Req request);

    /**
     * Sends a request and receives a response.
     *
     * @param request the request object to be sent
     * @return a Uni object representing the result of the send and receive operation
     */
    Uni<Message<Rep>> request(Message<Req> request);

    /**
     * Blocks until the consumer has been assigned all partitions for consumption.
     * If a {@code reply.partition} is provided, waits only for the assignment of that particular partition.
     * Otherwise, does a lookup for topic partitions and waits for the assignment of all partitions.
     *
     * @return a Uni object that resolves to a Set of TopicPartition once the assignments have been made.
     */
    Uni<Set<TopicPartition>> waitForAssignments();

    /**
     * Blocks until the consumer has been assigned given partitions for consumption.
     *
     * @param topicPartitions the number of partitions to wait for assignments
     * @return a Uni object that resolves to a Set of TopicPartition once the assignments have been made.
     */
    Uni<Set<TopicPartition>> waitForAssignments(Collection<TopicPartition> topicPartitions);

    /**
     * Retrieves the pending replies for each topic.
     *
     * @return a map containing the pending replies for each topic. The map's keys are the topic names and the values
     *         are instances of PendingReply.
     */
    Map<CorrelationId, PendingReply> getPendingReplies();

    /**
     * Retrieves the Kafka Consumer used for consuming messages.
     *
     * @return the Kafka Consumer used for consuming messages.
     */
    KafkaConsumer<?, Rep> getConsumer();

    /**
     * Sends the completion event to the channel indicating that no other events will be sent afterward.
     */
    void complete();

    /**
     * Calculates the partition header from the given byte array.
     *
     * @param bytes the byte array from which the partition header is calculated
     * @return the calculated partition header as an integer
     */
    static int replyPartitionFromBytes(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getInt();
    }

    /**
     * Converts the given partition header integer into a byte array.
     *
     * @param partition the partition header integer to be converted
     * @return the byte array representation of the partition header
     */
    static byte[] replyPartitionToBytes(int partition) {
        return ByteBuffer.allocate(4).putInt(partition).array();
    }
}
