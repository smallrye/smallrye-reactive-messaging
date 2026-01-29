package io.smallrye.reactive.messaging.mqtt.internal;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MqttTopicHelper {

    // Pattern for normal topic filters
    private static final Pattern TOPIC_FILTER_PATTERN = Pattern.compile("^([^#+]*|\\+|#)(/(([^#+]*|\\+)|#))*$");

    // Pattern for shared subscription: $share/{ShareName}/{filter}
    private static final Pattern SHARED_SUBSCRIPTION_PATTERN = Pattern.compile("^\\$share/([^/#+]+)/(.+)$");

    private MqttTopicHelper() {
        // avoid direct instantiation.
    }

    /**
     * Checks if a topic name matches a topic filter according to MQTT rules.
     *
     * Special rule: Topic names beginning with '$' don't match filters starting with '+' or '#'
     *
     * @param topicFilter the actual MQTT topic filter and relative Pattern
     * @param topicName the topic name to match
     * @return true if the topic name matches the filter
     */
    public static boolean matches(ActualTopicFilter topicFilter, String topicName) {

        if (topicName == null) {
            return false;
        }

        // Special rule: Topics starting with $ don't match wildcards at the beginning
        if (topicName.startsWith("$")) {
            if (topicFilter.topicFilter().startsWith("+") || topicFilter.topicFilter().startsWith("#")) {
                return false;
            }
        }

        return topicFilter.pattern().matcher(topicName).matches();
    }

    public static boolean matches(String topicFilter, String topicName) {
        return matches(topicFilterToPattern(topicFilter), topicName);
    }

    /**
     * Validates an MQTT topic filter (including shared subscriptions).
     *
     * @param topicFilter the topic filter to validate
     * @throws IllegalArgumentException if the topic filter is invalid
     */
    public static void validateTopicFilter(String topicFilter) {

        if (topicFilter == null) {
            throw new IllegalArgumentException("Topic filter cannot be null");
        }

        // Topic must be at least 1 character long
        if (topicFilter.length() < 1) {
            throw new IllegalArgumentException("Topic filter must be at least 1 character long");
        }

        // Maximum topic length: 65535 bytes
        if (topicFilter.length() > 65535) {
            throw new IllegalArgumentException("Topic filter exceeds maximum length of 65535 characters");
        }

        // Check if it's a Shared Subscription
        if (topicFilter.startsWith("$share/")) {
            validateSharedSubscription(topicFilter);
        } else {
            validateNormalTopicFilter(topicFilter);
        }

    }

    /**
     * Validates a shared subscription topic filter.
     * Format: $share/{ShareName}/{filter}
     *
     * @param topicFilter the shared subscription topic filter to validate
     * @throws IllegalArgumentException if the shared subscription format is invalid
     */
    private static void validateSharedSubscription(String topicFilter) {
        Matcher matcher = SHARED_SUBSCRIPTION_PATTERN.matcher(topicFilter);

        if (!matcher.matches()) {
            // Check specific errors for better error messages
            if (topicFilter.equals("$share/")) {
                throw new IllegalArgumentException("Shared subscription is missing ShareName and filter");
            }
            // Check if there's a second slash after "$share/"
            int secondSlashIndex = topicFilter.indexOf('/', 7); // 7 is the length of "$share/"
            if (secondSlashIndex == -1) {
                throw new IllegalArgumentException("Shared subscription is missing the filter part after ShareName");
            }
            throw new IllegalArgumentException("Invalid shared subscription format. Expected: $share/{ShareName}/{filter}");
        }

        String shareName = matcher.group(1);
        String filter = matcher.group(2);

        // [MQTT-4.8.2-1] ShareName must be at least 1 character long
        if (shareName.length() < 1) {
            throw new IllegalArgumentException(
                    "ShareName in shared subscription must be at least 1 character long [MQTT-4.8.2-1]");
        }

        // ShareName must NOT contain "/", "+" or "#"
        // (already verified by regex [^/#+]+, but we check for better error messages)
        if (shareName.contains("/")) {
            throw new IllegalArgumentException("ShareName cannot contain '/' character");
        }

        if (shareName.contains("+")) {
            throw new IllegalArgumentException("ShareName cannot contain '+' wildcard");
        }

        if (shareName.contains("#")) {
            throw new IllegalArgumentException("ShareName cannot contain '#' wildcard");
        }

        validateNormalTopicFilter(filter);
    }

    /**
     * Validates a normal (non-shared) MQTT topic filter.
     *
     * @param topicFilter the topic filter to validate
     * @throws IllegalArgumentException if the topic filter is invalid
     */
    private static void validateNormalTopicFilter(String topicFilter) {

        Matcher matcher = TOPIC_FILTER_PATTERN.matcher(topicFilter);

        if (!matcher.matches()) {
            throw new IllegalArgumentException("Topic filter contains invalid characters or structure");
        }

        // # can only appear as the last character
        int hashIndex = topicFilter.indexOf('#');
        if (hashIndex != -1) {
            if (hashIndex != topicFilter.length() - 1) {
                throw new IllegalArgumentException("Multi-level wildcard '#' must be the last character in the topic filter");
            }
            // # must be preceded by / or be the only character
            if (hashIndex > 0 && topicFilter.charAt(hashIndex - 1) != '/') {
                throw new IllegalArgumentException(
                        "Multi-level wildcard '#' must occupy an entire level (must be preceded by '/' or be alone)");
            }
        }

        // + and # must occupy an entire level
        String[] levels = topicFilter.split("/", -1);
        for (int i = 0; i < levels.length; i++) {
            String level = levels[i];
            if (level.contains("+") && !level.equals("+")) {
                throw new IllegalArgumentException(
                        "Single-level wildcard '+' must occupy an entire level at position " + i + ", found: '" + level + "'");
            }
            if (level.contains("#") && !level.equals("#")) {
                throw new IllegalArgumentException(
                        "Multi-level wildcard '#' must occupy an entire level at position " + i + ", found: '" + level + "'");
            }
        }
    }

    /**
     * Converts an MQTT topic filter to a Java regex pattern for matching topic names.
     *
     * Rules:
     * - '+' wildcard matches exactly one level (any characters except '/')
     * - '#' wildcard matches zero or more levels (must be at the end)
     * - All other characters are treated as literals and escaped
     * - Topic names starting with '$' don't match filters starting with '+' or '#'
     *
     * @param topicFilter the MQTT topic filter (e.g., "home/+/temperature", "sensor/#")
     * @return a compiled Pattern that matches topic names according to MQTT rules
     * @throws IllegalArgumentException if the topic filter is invalid
     */
    public static ActualTopicFilter topicFilterToPattern(String topicFilter) {

        // Handle shared subscriptions - extract the actual filter part
        if (topicFilter.startsWith("$share/")) {
            Matcher matcher = SHARED_SUBSCRIPTION_PATTERN.matcher(topicFilter);
            if (matcher.matches()) {
                topicFilter = matcher.group(2); // Extract the filter part
            }
        }

        StringBuilder regexBuilder = new StringBuilder("^");
        String[] levels = topicFilter.split("/", -1);

        for (int i = 0; i < levels.length; i++) {
            String level = levels[i];

            if (level.equals("+")) {
                // Single-level wildcard: matches any characters except '/'
                regexBuilder.append("[^/]*");
            } else if (level.equals("#")) {
                // Multi-level wildcard: matches everything remaining
                // Must be the last level (already validated)
                if (i == 0) {
                    // # at the beginning: matches everything
                    regexBuilder.append(".*");
                } else {
                    // # after some levels
                    // home/# should match "home", "home/kitchen", "home/kitchen/temp"
                    // The issue is that we already added "/" before this
                    // So we need to remove it and add the optional group
                    // Remove the last character (which is "/")
                    regexBuilder.setLength(regexBuilder.length() - 1);
                    // Now add the optional group that includes the /
                    regexBuilder.append("(/.*)?");
                }
            } else {
                // Literal level: escape special regex characters
                regexBuilder.append(Pattern.quote(level));
            }

            // Add '/' separator between levels (except after # or at the end)
            if (i < levels.length - 1 && !level.equals("#")) {
                regexBuilder.append("/");
            }
        }

        regexBuilder.append("$");
        Pattern pattern = Pattern.compile(regexBuilder.toString());

        return new ActualTopicFilter(topicFilter, pattern);
    }

}
