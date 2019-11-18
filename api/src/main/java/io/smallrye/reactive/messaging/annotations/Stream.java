package io.smallrye.reactive.messaging.annotations;

import static java.lang.annotation.ElementType.CONSTRUCTOR;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import javax.enterprise.util.Nonbinding;
import javax.inject.Qualifier;

import io.smallrye.reactive.messaging.Emitter;

/**
 * This qualifier indicates which channel should be injected / populated.
 * <p>
 * This qualifier can be used to inject a <em>stream</em> containing the items and signals propagated by the specified
 * channel. For example, it can be used to {@code @Inject} a {@code Publisher} representing a channel managed by the
 * Reactive Messaging implementation.
 * <p>
 * Can be injected:
 * <ul>
 * <li>Publisher&lt;X&gt; with X the payload type</li>
 * <li>Publisher&lt;Message&lt;X&gt;&gt; with X the payload type</li>
 * <li>Flowable&lt;X&gt; with X the payload type</li>
 * <li>Flowable&lt;Message&lt;X&gt;&gt; with X the payload type</li>
 * <li>PublisherBuilder&lt;Message&lt;X&gt;&gt; with X the payload type</li>
 * <li>PublisherBuilder&lt;X&gt; with X the payload type</li>
 * </ul>
 * <p>
 * When this qualifier is used on an {@link Emitter}, it indicates which channel received the emitted values / signals:
 *
 * <pre>
 * <code>
 * &#64;Inject @Stream("my-channel") Emitter&lt;String&gt; emitter;
 *
 * // ...
 * emitter.send("a").send("b").complete();
 * </code>
 * </pre>
 *
 * @deprecated use {@link Channel @Channel} instead"
 */
@Qualifier
@Retention(RetentionPolicy.RUNTIME)
@Target({ METHOD, CONSTRUCTOR, FIELD, PARAMETER })
@Deprecated
public @interface Stream {

    /**
     * The name of the stream (indicated in the {@code @Outgoing} annotation.
     *
     * @return the stream name, mandatory, non null and non-blank. It must matches one of the available streams.
     */
    @Nonbinding
    String value();
}
