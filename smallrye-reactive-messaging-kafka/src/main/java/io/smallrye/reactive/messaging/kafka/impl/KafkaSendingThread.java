package io.smallrye.reactive.messaging.kafka.impl;

import java.util.concurrent.atomic.AtomicInteger;

public class KafkaSendingThread extends Thread {

    private static final AtomicInteger threadCount = new AtomicInteger(0);

    public KafkaSendingThread(Runnable runnable) {
        super(runnable, "smallrye-kafka-producer-thread-" + threadCount.getAndIncrement());
    }
}
