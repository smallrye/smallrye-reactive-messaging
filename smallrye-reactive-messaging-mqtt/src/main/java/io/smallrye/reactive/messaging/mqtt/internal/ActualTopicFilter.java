package io.smallrye.reactive.messaging.mqtt.internal;

import java.util.regex.Pattern;

/**
 * A simple data holder that represents an MQTT topic filter and its
 * corresponding compiled regular-expression {@link Pattern}.
 *
 * <p>
 * The {@code topicFilter} is the original MQTT-style filter string
 * without $shared (and group) prefix (if any)
 * (for example, containing wildcards like <code>+</code> or <code>#</code>),
 * and {@code pattern} is a {@link Pattern} derived from that filter which can
 * be used at runtime to match actual MQTT topic names.
 *
 * <p>
 * Instances of this class are lightweight and intended for internal use
 * within the MQTT connector implementation as a pairing of the raw filter
 * and its ready-to-use compiled pattern.
 */
public record ActualTopicFilter(String topicFilter, Pattern pattern) {
}
