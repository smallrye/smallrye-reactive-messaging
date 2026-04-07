package io.smallrye.reactive.messaging.mqtt;

import io.smallrye.reactive.messaging.mqtt.session.MqttClientSession;

/**
 * A customizer applied to the {@link MqttClientSession} after creation.
 * <p>
 * Use this to set handlers on the session that cannot be configured via
 * {@link io.smallrye.reactive.messaging.mqtt.session.MqttClientSessionOptions},
 * such as the MQTT v5 enhanced authentication exchange handler.
 * <p>
 * Implementations must be CDI beans. Example:
 *
 * <pre>
 * &#64;ApplicationScoped
 * public class MyAuthCustomizer implements MqttClientSessionCustomizer {
 *     &#64;Override
 *     public void customize(MqttClientSession session) {
 *         session.authenticationExchangeHandler(msg -&gt; {
 *             // handle AUTH exchange
 *             session.authenticate(msg.reasonCode(), msg.properties());
 *         });
 *     }
 * }
 * </pre>
 */
public interface MqttClientSessionCustomizer {

    /**
     * Customize the given session.
     *
     * @param session the MQTT client session
     */
    void customize(MqttClientSession session);
}
