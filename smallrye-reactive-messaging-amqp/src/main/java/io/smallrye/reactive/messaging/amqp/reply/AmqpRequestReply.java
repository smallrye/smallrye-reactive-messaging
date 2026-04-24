package io.smallrye.reactive.messaging.amqp.reply;

import java.util.Map;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.common.annotation.Experimental;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.EmitterType;

/**
 * AmqpRequestReply provides functionality for sending requests and receiving
 * responses over AMQP 1.0.
 *
 * @param <Req> the type of the request value
 * @param <Rep> the type of the response value
 */
@Experimental("Experimental API")
public interface AmqpRequestReply<Req, Rep> extends EmitterType {

    String REPLY_TIMEOUT_KEY = "reply.timeout";

    String REPLY_CORRELATION_ID_HANDLER_KEY = "reply.correlation-id.handler";

    String DEFAULT_CORRELATION_ID_HANDLER = "uuid";

    String REPLY_FAILURE_HANDLER_KEY = "reply.failure.handler";

    String REPLY_ADDRESS_KEY = "reply.address";

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
     * @param request the request message to be sent
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
     * @param request the request message to be sent
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
