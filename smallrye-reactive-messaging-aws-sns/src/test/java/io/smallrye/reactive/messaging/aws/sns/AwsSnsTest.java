package io.smallrye.reactive.messaging.aws.sns;

import java.util.HashMap;
import java.util.Map;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.Flowable;

public class AwsSnsTest extends AwsSnsTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(AwsSnsTest.class);
    private WeldContainer container;
    String topic = "sns-test";
    String appUrl = "http://docker.for.mac.host.internal";
    int port = 8089;

    @Before
    public void initTest() {

        Weld weld = baseWeld();
        weld.addBeanClass(ConsumptionBean.class);
        addConfig(createSourceConfig(topic));

        container = weld.initialize();
        await(5);
        LOG.info("Initialized ...");
    }

    @After
    public void clearTest() {
        clear();
        container.shutdown();
        LOG.info("Cleanning ...");
    }

    @Test
    public void testSourceAndSink() {

        LOG.info("Sending message");
        send("Testing", topic);
        await(5);
        String msg = "Testing";
        Assert.assertEquals(msg, ConsumptionBean.msgReceived);
        LOG.info("Test source is done.");
    }

    private MapBasedConfig createSourceConfig(String topic) {

        String prefix = "mp.messaging.incoming.source.";
        Map<String, Object> config = new HashMap<>();
        config.put(prefix.concat("connector"), SnsConnector.CONNECTOR_NAME);
        config.put(prefix.concat("channel"), topic.concat("-transformed"));
        config.put(prefix.concat("port"), port);
        config.put("sns-app-host", appUrl);
        config.put("mock-sns-topics", true);
        config.put("sns-url", "http://localhost:9911");

        return new MapBasedConfig(config);
    }

    @SuppressWarnings("unchecked")
    private void send(String msg, String topic) {

        SubscriberBuilder<? extends Message<?>, Void> subscriber = createSinkSubscriber(topic);
        Flowable.fromArray(msg)
                .map(m -> Message.of(m))
                .safeSubscribe((Subscriber<Message<String>>) subscriber.build());
    }

    private SubscriberBuilder<? extends Message<?>, Void> createSinkSubscriber(String topic) {

        Map<String, Object> config = new HashMap<>();
        config.put("connector", SnsConnector.CONNECTOR_NAME);
        config.put("channel", topic.concat("-transformed"));

        SnsConnector snsConnector = new SnsConnector();
        snsConnector.initConnector();
        return snsConnector.getSubscriberBuilder(new MapBasedConfig(config));
    }
}
