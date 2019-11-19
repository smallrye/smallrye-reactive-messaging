package io.smallrye.reactive.messaging.jms;

import java.lang.IllegalStateException;
import java.util.concurrent.Executor;

import javax.jms.*;
import javax.json.bind.Jsonb;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.processors.UnicastProcessor;

public class JmsSource implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(JmsSource.class);
    private final Executor executor;
    private final PublisherBuilder<ReceivedJmsMessage<?>> source;
    private final Jsonb json;
    private JMSConsumer consumer;
    private final UnicastProcessor<ReceivedJmsMessage<?>> processor;

    JmsSource(JMSContext context, Config config, Jsonb json, Executor executor) {
        String name = config.getOptionalValue("destination", String.class)
                .orElseGet(() -> config.getValue("channel-name", String.class));

        this.executor = executor;
        this.json = json;

        String selector = config.getOptionalValue("selector", String.class).orElse(null);
        boolean nolocal = config.getOptionalValue("no-local", Boolean.TYPE).orElse(false);

        Destination destination = getDestination(context, name, config);

        if (config.getOptionalValue("durable", Boolean.TYPE).orElse(false)) {
            if (!(destination instanceof Topic)) {
                throw new IllegalStateException("Invalid destination, only topic can be durable");
            }
            consumer = context.createDurableConsumer((Topic) destination, name, selector, nolocal);
        } else {
            consumer = context.createConsumer(destination, selector, nolocal);
        }

        processor = UnicastProcessor.create(127, this::close);
        // TODO Support broadcast
        source = ReactiveStreams.fromPublisher(processor.doOnSubscribe(
                s -> executor.execute(this)));
    }

    public void close() {
        if (consumer != null) {
            consumer.close();
        }
    }

    @Override
    public void run() {
        while (true) {
            try {
                Message message = consumer.receive();
                if (message == null) {
                    return;
                }
                ReceivedJmsMessage msg = new ReceivedJmsMessage(message, executor, json);
                processor.onNext(msg);
            } catch (IllegalStateRuntimeException e) {
                LOGGER.warn("Unable to receive JMS messages - client has been closed");
                return;
            }
        }
    }

    private Destination getDestination(JMSContext context, String name, Config config) {
        String type = config.getOptionalValue("destination-type", String.class).orElse("queue");
        switch (type.toLowerCase()) {
            case "queue":
                LOGGER.info("Creating queue {}", name);
                return context.createQueue(name);
            case "topic":
                LOGGER.info("Creating topic {}", name);
                return context.createTopic(name);
            default:
                throw new IllegalArgumentException("Unknown destination type: " + type);
        }

    }

    PublisherBuilder<ReceivedJmsMessage<?>> getSource() {
        return source;
    }
}
