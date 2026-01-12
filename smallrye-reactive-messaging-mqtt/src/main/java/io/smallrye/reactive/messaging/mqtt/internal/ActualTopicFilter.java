package io.smallrye.reactive.messaging.mqtt.internal;

import java.util.regex.Pattern;

/**
 * A simple data holder that represents an MQTT topic filter and its
 * corresponding compiled regular-expression {@link Pattern}.
 *
 * <p>The {@code topicFilter} is the original MQTT-style filter string
 * without $shared (and group) prefix (if any)
 * (for example, containing wildcards like <code>+</code> or <code>#</code>),
 * and {@code pattern} is a {@link Pattern} derived from that filter which can
 * be used at runtime to match actual MQTT topic names.
 *
 * <p>Instances of this class are lightweight and intended for internal use
 * within the MQTT connector implementation as a pairing of the raw filter
 * and its ready-to-use compiled pattern.
 */
public class ActualTopicFilter {

    /**
     * The MQTT topic filter string as configured (may contain MQTT wildcards
     * such as <code>+</code> or <code>#</code>) but without $shared (and group) prefix (if any).
     */
    public String topicFilter;

    /**
     * The compiled {@link Pattern} derived from {@link #topicFilter} used to
     * match incoming MQTT topic names efficiently.
     */
    public Pattern pattern;

    /**
     * Construct a new {@link ActualTopicFilter}.
     *
     * @param topicFilter the original MQTT topic filter string without $shared (and group) prefix (if any)
     * @param pattern the compiled {@link Pattern} corresponding to the filter
     */
    public ActualTopicFilter(String topicFilter, Pattern pattern) {
        this.topicFilter = topicFilter;
        this.pattern = pattern;
    }

}
