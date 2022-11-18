package io.smallrye.reactive.messaging.mqtt.internal;

import java.util.regex.Pattern;

import io.vertx.mutiny.mqtt.messages.MqttPublishMessage;

public class MqttTopicHelper {

    private MqttTopicHelper() {
        // avoid direct instantiation.
    }

    /**
     * Escape special words in topic
     */
    public static String escapeTopicSpecialWord(String topic) {
        String[] specialWords = { "\\", "$", "(", ")", "*", ".", "[", "]", "?", "^", "{", "}", "|" };
        for (String word : specialWords) {
            if (topic.contains(word)) {
                topic = topic.replace(word, "\\" + word);
            }
        }
        return topic;
    }

    public static boolean matches(String topic, Pattern pattern, MqttPublishMessage m) {
        if (pattern != null) {
            return pattern.matcher(m.topicName()).matches();
        }
        return m.topicName().equals(topic);
    }
}
