package io.smallrye.reactive.messaging.jms;

import static io.smallrye.reactive.messaging.jms.i18n.JmsLogging.log;

import java.util.function.Function;
import java.util.function.Supplier;

import jakarta.jms.Destination;
import jakarta.jms.ExceptionListener;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;

public class JmsResourceHolder<T> implements ExceptionListener {

    private final String channel;
    private final Supplier<JMSContext> contextCreator;
    private Function<JmsResourceHolder<T>, Destination> destinationCreator;
    private Function<JmsResourceHolder<T>, T> clientCreator;
    private volatile JMSContext context;
    private volatile Destination destination;
    private volatile T client;

    private volatile boolean closed = false;

    public JmsResourceHolder(String channel, Supplier<JMSContext> contextCreator) {
        this.channel = channel;
        this.contextCreator = contextCreator;
    }

    public JmsResourceHolder<T> configure(Function<JmsResourceHolder<T>, Destination> destinationCreator,
            Function<JmsResourceHolder<T>, T> clientCreator) {
        this.destinationCreator = destinationCreator;
        this.clientCreator = clientCreator;
        return this;
    }

    public Destination getDestination() {
        if (destination == null) {
            synchronized (this) {
                if (destination == null) {
                    destination = destinationCreator.apply(this);
                }
            }
        }
        return destination;
    }

    public T getClient() {
        if (client == null) {
            synchronized (this) {
                if (client == null) {
                    client = clientCreator.apply(this);
                }
            }
        }
        return client;
    }

    public JMSContext getContext() {
        if (context == null) {
            synchronized (this) {
                if (context == null) {
                    context = contextCreator.get();
                    closed = false;
                    context.setExceptionListener(this);
                }
            }
        }
        return context;
    }

    @Override
    public void onException(JMSException exception) {
        System.out.println("Got exception: " + exception.getMessage());
        synchronized (this) {
            if (closed) {
                System.out.println("Already closed, ignoring exception");
                return;
            }
            log.jmsException(channel, exception);
            close();
        }
    }

    void close() {
        synchronized (this) {
            if (context != null) {
                context.close();
                context = null;
            }
            destination = null;
            if (client != null) {
                if (client instanceof AutoCloseable) {
                    try {
                        ((AutoCloseable) client).close();
                    } catch (Exception e) {
                        log.infof(e, "Error closing client for channel %s", channel);
                    }
                }
                client = null;
            }
            closed = true;
        }
    }
}
