package io.smallrye.reactive.messaging.rabbitmq.reply;

import java.util.Map;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.common.annotation.Experimental;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.EmitterType;

/**
 * RabbitMQRequestReply is a tech preview API that provides functionality for sending requests and receiving
 * responses over RabbitMQ.
 *
 * @param <Req> the type of the request value
 * @param <Rep> the type of the response value
 */
@Experimental("Experimental API")
public interface RabbitMQRequestReply<Req, Rep> extends EmitterType {

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
     * The Reply Failure Handler is responsible for extracting failure from the reply message.
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
     * Sends a request and receives responses.
     *
     * @param request the request object to be sent
     * @return a Multi object representing the results of the send and receive operation
     */
    Multi<Rep> requestMulti(Req request);

    /**
     * Sends a request and receives responses.
     *
     * @param request the request object to be sent
     * @return a Multi object representing the results of the send and receive operation
     */
    Multi<Message<Rep>> requestMulti(Message<Req> request);

    /**
     * Retrieves the pending replies.
     *
     * @return a map containing the pending replies. The map's keys are the correlation IDs and the values
     *         are instances of PendingReply.
     */
    Map<CorrelationId, PendingReply> getPendingReplies();

    /**
     * Sends the completion event to the channel indicating that no other events will be sent afterward.
     */
    void complete();

}
