package io.smallrye.reactive.messaging.aws.sns;

import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.amazonaws.services.sns.message.SnsNotification;

import io.reactivex.Flowable;
import io.reactivex.subscribers.DisposableSubscriber;

public class SnsMessage<T> implements org.eclipse.microprofile.reactive.messaging.Message<T> {

    private SnsNotification snsMessage;

    public SnsMessage(SnsNotification snsMessage) {

        Objects.requireNonNull(snsMessage, "SNS Message cannot be null.");
        this.snsMessage = snsMessage;
    }

    public SnsMessage(String msg) {
    }

    @Override
    public CompletionStage<Void> ack() {
        //Acknowledgment is handled automatically by AWS SDK.
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public T getPayload() {
        return snsMessage == null ? (T) "[Empty]" : (T) snsMessage.getMessage();
    }

    public String getSubject() {
        return snsMessage.getSubject();
    }

    public static void main(String[] args) {

        Queue<String> q = new ConcurrentLinkedQueue<>();
        q.add("s1");
        q.add("s2");

        Flowable.fromIterable(q).subscribeWith(new DisposableSubscriber<String>() {

            @Override
            public void onComplete() {
                // TODO Auto-generated method stub
                System.out.println("On complete");
            }

            @Override
            public void onError(Throwable arg0) {
                // TODO Auto-generated method stub
                System.out.println("On error");
            }

            @Override
            public void onNext(String arg0) {
                // TODO Auto-generated method stub
                System.out.println(arg0);
            }
        });

        CompletableFuture.supplyAsync(() -> {
            System.out.println("test");
            return "he3";
        });

    }
}
