package io.smallrye.reactive.messaging.connector;

import java.util.NoSuchElementException;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorFactory;
import org.reactivestreams.Publisher;

/**
 * SPI used to implement a connector managing a source of messages for a specific <em>transport</em>. For example, to
 * handle the consumption of records from Kafka, the reactive messaging extension would need to implement a {@code bean}
 * implementing this interface. This bean is called for every {@code channel} that needs to be created for this specific
 * <em>transport</em> (so Kafka in this case). These channels are connected to methods annotated with
 * {@link org.eclipse.microprofile.reactive.messaging.Incoming}.
 * <p>
 * Implementations are called to create a {@code channel} for each configured <em>transport</em>. The configuration is
 * done using MicroProfile Config. The following snippet gives an example for a hypothetical Kafka connector:
 *
 * <pre>
 * mp.messaging.incoming.my-channel.topic=my-topic
 * mp.messaging.connector.acme.kafka.bootstrap.servers=localhost:9092
 * ...
 * </pre>
 * <p>
 * The configuration keys are structured as follows: {@code mp.messaging.[incoming|outgoing].channel-name.attribute} or
 * {@code mp.messaging.[connector].connector-name.attribute}.
 * Channel names are not expected to contain {@code .} so the first occurrence of a {@code .} in the {@code channel-name}
 * portion of a property terminates the channel name and precedes the attribute name.
 * For connector attributes, the longest string, inclusive of {@code .}s, that matches a loadable
 * connector is used as a {@code connector-name}. The remainder, after a {@code .} separator, is the attribute name.
 * Configuration keys that begin {@code mp.messaging.outgoing}} are not used for {@link InboundConnector}
 * configuration.
 * <p>
 * The portion of the key that precedes the {@code attribute} acts as a property prefix that has a common structure
 * across all MicroProfile Reactive Messaging configuration properties.
 * </p>
 * <p>
 * The {@code channel-name} segment in the configuration key corresponds to the name of the channel used in the
 * {@code Incoming} annotation:
 *
 * <pre>
 * &#64;Incoming("my-channel")
 * public void consume(String s) {
 *     // ...
 * }
 * </pre>
 * <p>
 * The set of attributes depend on the connector and transport layer (for example, bootstrap.servers is Kafka specific).
 * The connector attribute indicates the name of the <em>connector</em>. It will be matched to
 * the value returned by the {@link Connector} qualifier used on the relevant {@link InboundConnector} bean
 * implementation.
 * This is how a smallrye reactive messaging implementation looks for the specific {@link InboundConnector} required for
 * a channel. Any {@code mp.messaging.connector} attributes for the channel's connector are also included in the set
 * of relevant attributes. Where an attribute is present for both a channel and its connector the value of the channel
 * specific attribute will take precedence.
 *
 * In the previous configuration, smallrye reactive messaging would need to find the
 * {@link InboundConnector} qualified using the {@link Connector} qualifier with the value
 * {@code acme.kafka} class to create the {@code my-channel} channel. Note that if the
 * connector cannot be found, the deployment must be failed with a {@link jakarta.enterprise.inject.spi.DeploymentException}.
 * <p>
 * The {@link #getPublisher(Config)} is called for every channel that needs to be created. The {@link Config} object
 * passed to the method contains a subset of the global configuration, and with the prefixes removed. So for the previous
 * configuration, it would be:
 *
 * <pre>
 * bootstrap.servers = localhost:9092
 * topic = my-topic
 * </pre>
 * <p>
 * In this example, if {@code topic} was missing as a configuration property, the Kafka connector would be at liberty to
 * default to the channel name indicated in the annotation as the Kafka topic. Such connector specific behaviours are
 * outside the scope of this specification.
 * <p>
 * So the connector implementation can retrieve the value with {@link Config#getValue(String, Class)} and
 * {@link Config#getOptionalValue(String, Class)}.
 * <p>
 * If the configuration is invalid, the {@link #getPublisher(Config)} method must throw an
 * {@link IllegalArgumentException}, caught by smallrye reactive messaging and failing the deployment by
 * throwing a {@link jakarta.enterprise.inject.spi.DeploymentException} wrapping the exception.
 * <p>
 *
 * This class is specific to SmallRye and is uses internally instead of
 * {@link org.eclipse.microprofile.reactive.messaging.spi.IncomingConnectorFactory}.
 * Instead of a {@link org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder}, it returns a
 * {@link org.reactivestreams.Publisher}.
 */
public interface InboundConnector extends ConnectorFactory {

    /**
     * Creates a <em>channel</em> for the given configuration. The channel's configuration is associated with a
     * specific {@code connector}, using the {@link Connector} qualifier's parameter indicating a key to
     * which {@link InboundConnector} to use.
     *
     * <p>
     * Note that the connection to the <em>transport</em> or <em>broker</em> is generally postponed until the
     * subscription occurs.
     *
     * @param config the configuration, must not be {@code null}, must contain the {@link #CHANNEL_NAME_ATTRIBUTE}
     *        attribute.
     * @return the created {@link org.reactivestreams.Publisher}, will not be {@code null}.
     * @throws IllegalArgumentException if the configuration is invalid.
     * @throws NoSuchElementException if the configuration does not contain an expected attribute.
     */
    Publisher<? extends Message<?>> getPublisher(Config config);

}
