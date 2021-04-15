package io.smallrye.reactive.messaging.kafka.impl;

import java.util.concurrent.atomic.AtomicInteger;

public class KafkaPollingThread extends Thread {

    private static final AtomicInteger threadCount = new AtomicInteger(0);

    public KafkaPollingThread(Runnable runnable) {
        super(runnable, "smallrye-kafka-consumer-thread-" + threadCount.getAndIncrement());
    }
}
