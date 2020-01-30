package io.smallrye.reactive.messaging.kafka.conflict;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.spi.DeploymentException;

import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.After;
import org.junit.Test;

import io.reactivex.Flowable;
import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.reactive.messaging.annotations.Merge;
import io.smallrye.reactive.messaging.kafka.KafkaTestBase;
import io.smallrye.reactive.messaging.kafka.MapBasedConfig;

/**
 * Reproducer for https://github.com/smallrye/smallrye-reactive-messaging/issues/373.
 */
public class ChannelNameConflictTest extends KafkaTestBase {

    private final static Map<String, Object> CONFLICT = new HashMap<>();

    static {
        CONFLICT.put("mp.messaging.incoming.my-topic.connector", "smallrye-kafka");
        CONFLICT.put("mp.messaging.incoming.my-topic.topic", "my-topic-1");
        CONFLICT.put("mp.messaging.incoming.my-topic.value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");

        CONFLICT.put("mp.messaging.outgoing.my-topic.connector", "smallrye-kafka");
        CONFLICT.put("mp.messaging.outgoing.my-topic.topic", "my-topic-1");
        CONFLICT.put("mp.messaging.outgoing.my-topic.value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
    }

    private WeldContainer container;

    @After
    public void cleanup() {
        if (container != null) {
            container.close();
        }
        // Release the config objects
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
    }

    @Test(expected = DeploymentException.class)
    public void testWhenBothIncomingAndOutgoingUseTheSameName() {
        new MapBasedConfig(CONFLICT).write();
        Weld weld = baseWeld();
        weld.addBeanClass(Bean.class);
        container = weld.initialize();
    }

    @ApplicationScoped
    public static class Bean {

        @Outgoing("my-topic")
        public Flowable<String> publisher() {
            return Flowable.timer(10, TimeUnit.MILLISECONDS)
                    .map(l -> Long.toString(l))
                    .take(20);
        }

        private List<String> list = new CopyOnWriteArrayList<>();

        @Incoming("my-topic")
        @Merge
        public void consumer(String input) {
            list.add(input);
        }

        public List<String> getList() {
            return list;
        }
    }

}
