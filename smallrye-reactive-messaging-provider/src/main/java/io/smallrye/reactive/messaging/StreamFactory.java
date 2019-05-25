package io.smallrye.reactive.messaging;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.Map;
import java.util.concurrent.CompletionStage;

public interface StreamFactory {

  PublisherBuilder<? extends Message> createPublisherBuilder(String type, Config config);

  SubscriberBuilder<? extends Message, Void> createSubscriberBuilder(String type, Config config);
}
