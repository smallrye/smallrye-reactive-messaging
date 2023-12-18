package io.smallrye.reactive.messaging.decorator;

import java.util.Arrays;
import java.util.List;

import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;

public class EmitterBean {

    public static final List<String> TEST_STRINGS = Arrays.asList("foo", "bar", "baz");

    @Inject
    @Channel("sink")
    Emitter<String> emitter;

    public void sendStrings() {
        for (String msg : TEST_STRINGS) {
            emitter.send(msg);
        }
    }

}
