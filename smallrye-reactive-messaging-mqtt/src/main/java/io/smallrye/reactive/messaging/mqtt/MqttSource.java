package io.smallrye.reactive.messaging.mqtt;

import static io.smallrye.reactive.messaging.mqtt.i18n.MqttExceptions.ex;
import static io.smallrye.reactive.messaging.mqtt.i18n.MqttLogging.log;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;

import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt3.message.publish.Mqtt3Publish;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.converters.multi.MultiRxConverters;

public class MqttSource {

    private final PublisherBuilder<MqttMessage<?>> source;
    private final AtomicBoolean subscribed = new AtomicBoolean();
    private final Pattern pattern;

    public MqttSource(MqttConnectorIncomingConfiguration config) {
        MqttConnectorIncomingConfiguration options = config;

        String topic = config.getTopic().orElseGet(config::getChannel);
        int qos = config.getQos();
        boolean broadcast = config.getBroadcast();
        MqttFailureHandler.Strategy strategy = MqttFailureHandler.Strategy.from(config.getFailureStrategy());
        MqttFailureHandler onNack = createFailureHandler(strategy, config.getChannel());

        if (topic.contains("#") || topic.contains("+")) {
            String replace = topic.replace("+", "[^/]+")
                    .replace("#", ".+");
            pattern = Pattern.compile(replace);
        } else {
            pattern = null;
        }

        Clients.ClientHolder holder = Clients.getHolder(options);

        this.source = ReactiveStreams.fromPublisher(
                holder.connect()
                        .onItem()
                        .transformToMulti(client -> {
                            return Multi.createFrom()
                                    .converter(MultiRxConverters.fromFlowable(), client.subscribePublishesWith()
                                            .topicFilter(topic).qos(MqttQos.fromCode(qos))
                                            .applySubscribe().doOnSingle(subAck -> {
                                                subscribed.set(true);
                                            }))
                                    //TODO: do we really need this ?
                                    .filter(m -> matches(topic, m))
                                    .onItem()
                                    .transform(x -> new ReceivingMqttMessage(x, onNack));
                        })
                        .stage(multi -> {
                            if (broadcast) {
                                return multi.broadcast().toAllSubscribers();
                            }
                            return multi;
                        })
                        .onCancellation().invoke(() -> subscribed.set(false))
                        .onFailure().invoke(log::unableToConnectToBroker));
    }

    private boolean matches(String topic, Mqtt3Publish m) {
        String topicName = m.getTopic().toString();
        if (pattern != null) {
            return pattern.matcher(topicName).matches();
        }
        return topicName.equals(topic);
    }

    private MqttFailureHandler createFailureHandler(MqttFailureHandler.Strategy strategy, String channel) {
        switch (strategy) {
            case IGNORE:
                return new MqttIgnoreFailure(channel);
            case FAIL:
                return new MqttFailStop(channel);
            default:
                throw ex.illegalArgumentUnknownStrategy(strategy.toString());
        }
    }

    PublisherBuilder<MqttMessage<?>> getSource() {
        return source;
    }

    boolean isSubscribed() {
        return subscribed.get();
    }
}
