package io.smallrye.reactive.messaging.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

import javax.enterprise.util.Nonbinding;
import javax.inject.Qualifier;

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
 */
@Qualifier
@Retention(RetentionPolicy.RUNTIME)
public @interface Channel {

    /**
     * The name of the stream (indicated in the {@code @Outgoing} annotation.
     *
     * @return the stream name, mandatory, non null and non-blank. It must matches one of the available streams.
     */
    @Nonbinding
    String value();
}
