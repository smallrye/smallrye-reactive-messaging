package io.smallrye.reactive.messaging.aws.sqs;

import java.net.URI;
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.atomic.AtomicReference;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.helpers.Subscriptions;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

public class SqsReceiver implements Flow.Publisher<Message<?>>, Flow.Subscription {
    private final AtomicReference<Subscriber<? super Message<?>>> downstream = new AtomicReference<>();

    private final SqsConfig config;

    private final SqsClient client;

    private final String queueUrl;

    public SqsReceiver(SqsConfig config) {
        this.config = config;
        var clientBuilder = SqsClient.builder();
        if (config.getEndpointOverride().isPresent()) {
            clientBuilder.endpointOverride(URI.create(config.getEndpointOverride().get()));
        }
        if (config.getRegion().isPresent()) {
            clientBuilder.region(config.getRegion().get());
        }
        this.client = clientBuilder.build();
        this.queueUrl = client.getQueueUrl(r -> r.queueName(config.getQueueName())).queueUrl();
    }

    @Override
    public void request(long n) {
        var receiveRequest = ReceiveMessageRequest
                .builder().queueUrl(this.queueUrl)
                .waitTimeSeconds(config.getWaitTimeSeconds())
                .maxNumberOfMessages((int) n)
                .build();
        var messages = client.receiveMessage(receiveRequest).messages();

        messages.forEach(m -> downstream.get().onNext(new SqsMessage(m)));
    }

    @Override
    public void cancel() {
        var subscriber = downstream.getAndSet(null);
        if (subscriber != null) {
            subscriber.onComplete();
        }
        this.client.close();
    }

    @Override
    public void subscribe(Flow.Subscriber<? super Message<?>> s) {
        if (downstream.compareAndSet(null, s)) {
            s.onSubscribe(this);
        } else {
            // TODO: use ex
            Subscriptions.fail(s, new Exception("Subscriber already registered"));
        }
    }

}
