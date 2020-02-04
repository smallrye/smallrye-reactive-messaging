/*******************************************************************************
 * Copyright (c) 2018 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file(s) distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package io.smallrye.reactive.messaging.tck;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.ProcessorBuilder;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * Convenience helper for holding sent messages so that assertions can be run against them.
 */
public class MockedSender<T> {

    private final Deque<Message<T>> queue = new ArrayDeque<>();
    private final List<MessageProcessor> publishers = new CopyOnWriteArrayList<>();

    public int numPublishers() {
        return publishers.size();
    }

    public ProcessorBuilder<Void, Message<T>> createWrappedProcessor() {
        return ReactiveStreams.fromProcessor(new MessageProcessor());
    }

    public ProcessorBuilder<Void, T> createProcessor() {
        return createWrappedProcessor().map(Message::getPayload);
    }

    public PublisherBuilder<Message<T>> createWrappedPublisher() {
        return ReactiveStreams.fromPublisher(new MessageProcessor());
    }

    public PublisherBuilder<T> createPublisher() {
        return createWrappedPublisher().map(Message::getPayload);
    }

    public void send(T... message) {
        queue.addAll(Arrays.stream(message).map(payload -> Message.<T>newBuilder().payload(payload).build()).collect(Collectors.toList()));
        trySend();
    }

    public void send(Message<T>... message) {
        queue.addAll(Arrays.asList(message));
        trySend();
    }

    private void trySend() {
        for (MessageProcessor publisher : publishers) {
            publisher.maybeSend(0);
            if (queue.isEmpty()) {
                break;
            }
        }
    }

    public void completeAll() {
        List<MessageProcessor> all = new ArrayList<>(publishers);
        all.clear();
        for (MessageProcessor publisher : all) {
            publisher.onComplete();
        }
    }

    public void failAll(Throwable error) {
        List<MessageProcessor> all = new ArrayList<>(publishers);
        all.clear();
        for (MessageProcessor publisher : all) {
            publisher.onError(error);
        }
    }

    private class MessageProcessor implements Processor<Void, Message<T>>, Subscription {

        private final AtomicReference<Subscriber<? super Message<T>>> subscriber = new AtomicReference<>();
        private long demand = 0;
        private boolean sending = false;

        @Override
        public void subscribe(Subscriber<? super Message<T>> subscriber) {
            if (!this.subscriber.compareAndSet(null, subscriber)) {
                subscriber.onSubscribe(new Subscription() {
                    @Override
                    public void request(long l) {
                    }

                    @Override
                    public void cancel() {
                    }
                });
                subscriber.onError(new RuntimeException("I only support one subscriber"));
            } else {
                publishers.add(this);
                subscriber.onSubscribe(this);
            }
        }

        private synchronized void maybeSend(long increaseDemand) {
            demand += increaseDemand;
            if (demand < 0) {
                demand = Long.MAX_VALUE;
            }
            if (!sending) {
                sending = true;
                while (demand > 0) {
                    Message<T> toSend = queue.poll();
                    if (toSend != null) {
                        this.subscriber.get().onNext(toSend);
                        if (demand != Long.MAX_VALUE) {
                            demand--;
                        }
                    } else {
                        break;
                    }
                }
                sending = false;
            }
        }

        @Override
        public void request(long l) {
            maybeSend(l);
        }

        @Override
        public void cancel() {
            publishers.remove(this);
        }

        @Override
        public void onSubscribe(Subscription subscription) {
        }

        @Override
        public void onNext(Void aVoid) {
        }

        @Override
        public void onError(Throwable throwable) {
        }

        @Override
        public void onComplete() {
        }
    }
}
