package io.smallrye.reactive.messaging.memory;

/**
 * Allows interacting with a test incoming channel.
 * Use this API to inject messages into the application during tests.
 *
 * @param <T> the type of payload or message.
 */
public interface TestIncoming<T> {

    /**
     * @return the channel name.
     */
    String name();

    /**
     * Delivers a message or a payload to the application via this channel.
     *
     * @param messageOrPayload the message or payload to deliver. In the case of a payload, a simple message is created.
     *        Must not be {@code null}
     * @return this to allow chaining calls.
     */
    TestIncoming<T> deliver(T messageOrPayload);

    /**
     * Delivers a payload with metadata to the application via this channel.
     * A {@link org.eclipse.microprofile.reactive.messaging.Message} is created with the given payload
     * and metadata objects attached.
     * <p>
     * This is useful for simulating connector-specific metadata in tests, for example:
     *
     * <pre>{@code
     * incoming.deliver(new Order(...),
     *     IncomingKafkaRecordMetadata.builder().withTopic("orders").withPartition(0).build());
     * }</pre>
     *
     * @param payload the payload to deliver, must not be {@code null}
     * @param metadata the metadata objects to attach to the message
     * @return this to allow chaining calls.
     */
    TestIncoming<T> deliver(T payload, Object... metadata);

    /**
     * The flag to enable dispatching messages on Vert.x context.
     *
     * @param runOnVertxContext whether to dispatch messages on Vert.x context or not
     * @return this to allow chaining calls.
     */
    TestIncoming<T> runOnVertxContext(boolean runOnVertxContext);

    /**
     * Sends the completion event.
     */
    void complete();

    /**
     * Sends a failure.
     *
     * @param failure the failure, must not be {@code null}
     */
    void fail(Throwable failure);
}
