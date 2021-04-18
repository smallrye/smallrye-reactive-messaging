package io.smallrye.reactive.messaging.mqtt.server;

class TestMqttMessage {

    private final String topic;
    private final int id;
    private final String body;
    private final int qos;
    private final boolean retained;

    TestMqttMessage(String topic, int id, String body, int qos, boolean retained) {
        this.topic = topic;
        this.id = id;
        this.body = body;
        this.qos = qos;
        this.retained = retained;
    }

    String getTopic() {
        return topic;
    }

    String getBody() {
        return body;
    }

    int getQos() {
        return qos;
    }

    boolean isRetained() {
        return retained;
    }

    int getId() {
        return id;
    }
}
