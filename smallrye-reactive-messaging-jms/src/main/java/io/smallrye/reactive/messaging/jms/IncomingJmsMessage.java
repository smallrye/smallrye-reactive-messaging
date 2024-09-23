package io.smallrye.reactive.messaging.jms;

import static io.smallrye.reactive.messaging.jms.i18n.JmsExceptions.ex;
import static io.smallrye.reactive.messaging.providers.locals.ContextAwareMessage.captureContextMetadata;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Function;

import jakarta.jms.JMSException;
import jakarta.jms.Message;

import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.jms.fault.JmsFailureHandler;
import io.smallrye.reactive.messaging.json.JsonMapping;
import io.smallrye.reactive.messaging.providers.locals.ContextAwareMessage;

public class IncomingJmsMessage<T> implements ContextAwareMessage<T> {
    private final Message delegate;
    private final Executor executor;
    private final Class<T> clazz;
    private final JsonMapping jsonMapping;
    private final IncomingJmsMessageMetadata jmsMetadata;
    private final Metadata metadata;
    private final JmsFailureHandler failureHandler;

    IncomingJmsMessage(Message message, Executor executor, JsonMapping jsonMapping, JmsFailureHandler failureHandler) {
        this.delegate = message;
        this.jsonMapping = jsonMapping;
        this.executor = executor;
        this.failureHandler = failureHandler;
        String cn = null;
        try {
            cn = message.getStringProperty("_classname");
            if (cn == null) {
                cn = message.getJMSType();
            }
        } catch (JMSException e) {
            // ignore it
        }
        try {
            this.clazz = cn != null && !cn.isEmpty() ? load(cn) : null;
        } catch (ClassNotFoundException e) {
            throw ex.illegalStateUnableToLoadClass(e);
        }

        this.jmsMetadata = new IncomingJmsMessageMetadata(message);
        this.metadata = captureContextMetadata(this.jmsMetadata);
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
        return (Class<T>) IncomingJmsMessage.class.getClassLoader().loadClass(cn);
    }

    @Override
    @SuppressWarnings("unchecked")
    public T getPayload() {
        try {
            if (clazz != null) {
                return convert(delegate.getBody(String.class));
            } else {
                return (T) delegate.getBody(Object.class);
            }
        } catch (JMSException e) {
            throw new IllegalStateException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private T convert(String value) {
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

    @Override
    public Function<Metadata, CompletionStage<Void>> getAckWithMetadata() {
        return this::ack;
    }

    @Override
    public CompletionStage<Void> ack(Metadata metadata) {
        return Uni.createFrom().voidItem()
                .onItem().invoke(m -> {
                    try {
                        delegate.acknowledge();
                    } catch (JMSException e) {
                        throw new IllegalArgumentException("Unable to acknowledge message", e);
                    }
                })
                .runSubscriptionOn(executor)
                .emitOn(this::runOnMessageContext)
                .subscribeAsCompletionStage();
    }

    @Override
    public Metadata getMetadata() {
        return metadata;
    }

    @Override
    public CompletionStage<Void> nack(Throwable reason, Metadata metadata) {
        return failureHandler.handle(this, reason, metadata).subscribeAsCompletionStage();
    }

    @SuppressWarnings({ "unchecked" })
    @Override
    public <C> C unwrap(Class<C> unwrapType) {
        if (Message.class.equals(unwrapType)) {
            return (C) delegate;
        }
        if (IncomingJmsMessageMetadata.class.equals(unwrapType)) {
            return (C) jmsMetadata;
        }
        throw ex.illegalStateUnableToUnwrap(unwrapType);
    }

}
