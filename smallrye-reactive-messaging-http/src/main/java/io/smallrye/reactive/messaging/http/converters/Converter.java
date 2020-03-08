package io.smallrye.reactive.messaging.http.converters;

import io.smallrye.mutiny.Uni;

public interface Converter<I, O> {

    Uni<O> convert(I payload);

    Class<? extends I> input();

    Class<? extends O> output();

}
