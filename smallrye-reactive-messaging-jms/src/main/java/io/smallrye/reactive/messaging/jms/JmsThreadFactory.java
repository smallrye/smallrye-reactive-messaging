package io.smallrye.reactive.messaging.jms;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

class JmsThreadFactory implements ThreadFactory {

    private final AtomicInteger count = new AtomicInteger();
    private final String prefix;

    JmsThreadFactory(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public Thread newThread(Runnable r) {
        return new Thread(r, prefix + "-" + count.getAndIncrement());
    }
}
