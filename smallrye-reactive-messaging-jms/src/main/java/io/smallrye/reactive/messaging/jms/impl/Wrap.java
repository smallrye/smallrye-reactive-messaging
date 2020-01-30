package io.smallrye.reactive.messaging.jms.impl;

import java.util.concurrent.Callable;

public class Wrap {

    private Wrap() {
        // avoid direct instantiation
    }

    public static <R> R wrap(Callable<R> code) {
        try {
            return code.call();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
