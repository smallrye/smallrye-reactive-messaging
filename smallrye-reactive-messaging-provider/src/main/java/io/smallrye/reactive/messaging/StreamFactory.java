package io.smallrye.reactive.messaging;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.Map;
import java.util.concurrent.CompletionStage;

public interface StreamFactory {


  CompletionStage<Publisher<? extends Message>> createPublisherAndRegister(String name, Map<String, String> config);

  CompletionStage<Subscriber<? extends Message>> createSubscriberAndRegister(String name, Map<String, String> config);

  CompletionStage<Publisher<? extends Message>> createPublisher(String type, Map<String, String> config);

  CompletionStage<Subscriber<? extends Message>> createSubscriber(String type,  Map<String, String> config);
}
