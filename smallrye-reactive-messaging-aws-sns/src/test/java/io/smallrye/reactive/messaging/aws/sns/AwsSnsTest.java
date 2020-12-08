package io.smallrye.reactive.messaging.aws.sns;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.reactivestreams.Subscriber;

import io.restassured.RestAssured;
import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class AwsSnsTest extends AwsSnsTestBase {

    private WeldContainer container;
    private final String topic = "sns-test";

    @Before
    public void initTest() {
        System.setProperty("aws.region", "some-region");
        System.setProperty("aws.accessKeyId", "some-key-id");
        System.setProperty("aws.secretAccessKey", "some-access-id");
        Weld weld = baseWeld();
        weld.addBeanClass(ConsumptionBean.class);
        addConfig(createSourceConfig(topic));

        container = weld.initialize();

        await()
                .pollDelay(1, TimeUnit.SECONDS)
                .until(() -> {
                    try {
                        URL url = new URL("http://localhost:" + 8389);
                        RestAssured.head(url).then().statusCode(204);
                        return true;
                    } catch (Exception e) {
                        return false;
                    }
                });
    }

    @After
    public void clearTest() {
        System.clearProperty("aws.region");
        System.clearProperty("aws.accessKeyId");
        System.clearProperty("aws.secretAccessKey");

        clear();
        container.shutdown();
    }

    @Test
    public void testSourceAndSink() {
        if (!isTestSupported()) {
            Logger.getAnonymousLogger().warning("Test not supported on this platform - skipping");
            return;
        }
        ConsumptionBean bean = container.select(ConsumptionBean.class).get();
        send("Hello-0", topic);
        await().until(() -> bean.messages().size() == 1);
        assertThat(bean.messages().get(0)).isEqualTo("Hello-0");
        for (int i = 1; i < 11; i++) {
            send("Hello-" + i, topic);
        }
        await().until(() -> bean.messages().size() == 11);
        assertThat(bean.messages()).allSatisfy(s -> assertThat(s).startsWith("Hello"));
    }

    private MapBasedConfig createSourceConfig(String topic) {
        String prefix = "mp.messaging.incoming.source.";
        Map<String, Object> config = new HashMap<>();
        config.put(prefix.concat("connector"), SnsConnector.CONNECTOR_NAME);
        config.put(prefix.concat("topic"), topic.concat("-transformed"));
        String appUrl = getAppUrl();
        int port = 8389;
        config.put(prefix.concat("port"), port);
        config.put(prefix.concat("host"), appUrl);
        config.put(prefix.concat("mock-sns-topics"), true);
        config.put("sns-url", String.format("http://%s:%d", ip(), port()));
        return new MapBasedConfig(config);
    }

    private boolean isTestSupported() {
        String os = System.getProperty("os.name").toLowerCase();
        return os.contains("mac");
    }

    private String getAppUrl() {
        String os = System.getProperty("os.name").toLowerCase();
        if (os.contains("win")) {
            throw new UnsupportedOperationException("Windows not supported");
        } else if (os.contains("mac")) {
            return "http://docker.for.mac.host.internal";
        } else {
            return "localhost";
        }
    }

    @SuppressWarnings("unchecked")
    private void send(String msg, String topic) {
        SubscriberBuilder<? extends Message<?>, Void> subscriber = createSinkSubscriber(topic);
        Multi.createFrom().item(msg)
                .map(Message::of)
                .subscribe((Subscriber<Message<String>>) subscriber.build());
    }

    private SubscriberBuilder<? extends Message<?>, Void> createSinkSubscriber(String topic) {
        Map<String, Object> config = new HashMap<>();
        config.put("connector", SnsConnector.CONNECTOR_NAME);
        config.put("topic", topic.concat("-transformed"));
        config.put("sns-url", String.format("http://%s:%d", ip(), port()));
        SnsConnector snsConnector = new SnsConnector();
        snsConnector.setup(executionHolder);
        snsConnector.initConnector();
        return snsConnector.getSubscriberBuilder(new MapBasedConfig(config));
    }
}
