package io.smallrye.reactive.messaging.aws.sqs;

import static io.smallrye.reactive.messaging.providers.locals.ContextAwareMessage.captureContextMetadata;

import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.reactive.messaging.aws.sqs.i18n.AwsSqsExceptions;
import io.smallrye.reactive.messaging.json.JsonMapping;
import io.smallrye.reactive.messaging.providers.MetadataInjectableMessage;
import io.smallrye.reactive.messaging.providers.locals.ContextAwareMessage;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;

/**
 * Represents a message received from SQS.
 * <p>
 * Not to be used directly by user applications to consume messages.
 *
 * @param <T> the type of the payload
 */
public class SqsMessage<T> implements ContextAwareMessage<T>, MetadataInjectableMessage<T> {

    private final Message message;
    private final SqsAckHandler ackHandler;
    private final T payload;
    private Metadata metadata;

    public SqsMessage(Message message, JsonMapping jsonMapper, SqsAckHandler ackHandler) {
        this.message = message;
        this.ackHandler = ackHandler;
        Map<String, MessageAttributeValue> attributes = message.messageAttributes();
        if (attributes.containsKey(SqsConnector.CLASS_NAME_ATTRIBUTE)) {
            String cn = attributes.get(SqsConnector.CLASS_NAME_ATTRIBUTE).stringValue();
            try {
                this.payload = convert(message.body(), jsonMapper, load(cn));
            } catch (ClassNotFoundException e) {
                throw AwsSqsExceptions.ex.illegalStateUnableToLoadClass(cn, e);
            }
        } else {
            this.payload = (T) message.body();
        }

        this.metadata = captureContextMetadata(new SqsIncomingMetadata(message));
    }

    @SuppressWarnings("unchecked")
    private T convert(String value, JsonMapping jsonMapping, Class<T> clazz) {
        if (clazz.equals(Integer.class)) {
            return (T) Integer.valueOf(value);
        }
        if (clazz.equals(Long.class)) {
            return (T) Long.valueOf(value);
        }
        if (clazz.equals(Double.class)) {
            return (T) Double.valueOf(value);
        }
        if (clazz.equals(Float.class)) {
            return (T) Float.valueOf(value);
        }
        if (clazz.equals(Boolean.class)) {
            return (T) Boolean.valueOf(value);
        }
        if (clazz.equals(Short.class)) {
            return (T) Short.valueOf(value);
        }
        if (clazz.equals(Byte.class)) {
            return (T) Byte.valueOf(value);
        }
        if (clazz.equals(String.class)) {
            return (T) value;
        }

        return jsonMapping.fromJson(value, clazz);
    }

    @SuppressWarnings("unchecked")
    private Class<T> load(String cn) throws ClassNotFoundException {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        if (loader != null) {
            try {
                return (Class<T>) loader.loadClass(cn);
            } catch (ClassNotFoundException e) {
                // Will try with the current class classloader
            }
        }
        return (Class<T>) SqsMessage.class.getClassLoader().loadClass(cn);
    }

    @Override
    public Metadata getMetadata() {
        return metadata;
    }

    @Override
    public T getPayload() {
        return payload;
    }

    public Message getMessage() {
        return message;
    }

    @Override
    public CompletionStage<Void> ack(Metadata metadata) {
        return ackHandler.handle(this).subscribeAsCompletionStage();
    }

    @Override
    public Function<Metadata, CompletionStage<Void>> getAckWithMetadata() {
        return this::ack;
    }

    @Override
    public CompletionStage<Void> nack(Throwable reason, Metadata metadata) {
        return ackHandler.handle(this).subscribeAsCompletionStage();
    }

    @Override
    public BiFunction<Throwable, Metadata, CompletionStage<Void>> getNackWithMetadata() {
        return this::nack;
    }

    @Override
    public void injectMetadata(Object metadataObject) {
        this.metadata = this.metadata.with(metadataObject);
    }
}
