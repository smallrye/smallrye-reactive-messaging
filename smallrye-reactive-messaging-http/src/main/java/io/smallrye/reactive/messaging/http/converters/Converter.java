package io.smallrye.reactive.messaging.http.converters;

import java.util.concurrent.CompletionStage;

public interface Converter<I, O> {

    CompletionStage<O> convert(I payload);

    Class<? extends I> input();

    Class<? extends O> ouput();

}
