package io.smallrye.reactive.messaging.camel.documentation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Test;

import io.smallrye.reactive.messaging.camel.CamelTestBase;
import io.smallrye.reactive.messaging.camel.MapBasedConfig;

public class DocumentationTest extends CamelTestBase {

    private final Path orders = new File("target/orders").toPath();
    private final Path prices = new File("target/prices").toPath();

    @After
    public void deleteDirectory() {
        File file = orders.toFile();
        File[] files = file.listFiles();
        if (files != null) {
            for (File f : files) {
                f.delete();
            }
        }

        file = prices.toFile();
        files = file.listFiles();
        if (files != null) {
            for (File f : files) {
                f.delete();
            }
        }
    }

    @Test
    public void testFileConsumer() {
        orders.toFile().mkdirs();

        addClasses(FileConsumer.class);
        addConfig(getConsumerConfig());
        initialize();

        new Thread(() -> {
            for (int i = 0; i < 10; i++) {
                File out = new File(orders.toFile(), "file-" + i);
                String text = "hello-" + i;
                try {
                    Files.write(out.toPath(), text.getBytes(StandardCharsets.UTF_8));
                } catch (IOException e) {
                    e.printStackTrace();
                    // Ignore it.
                }
            }
        }).start();

        FileConsumer bean = bean(FileConsumer.class);
        await().until(() -> bean.list().size() == 10);
        assertThat(bean.list()).allSatisfy(s -> assertThat(s).startsWith("file-"));
    }

    @Test
    public void testFileMessageConsumer() {
        orders.toFile().mkdirs();

        addClasses(FileMessageConsumer.class);
        addConfig(getConsumerConfig());
        initialize();

        new Thread(() -> {
            for (int i = 0; i < 10; i++) {
                File out = new File(orders.toFile(), "file-" + i);
                String text = "hello-" + i;
                try {
                    Files.write(out.toPath(), text.getBytes(StandardCharsets.UTF_8));
                } catch (IOException e) {
                    e.printStackTrace();
                    // Ignore it.
                }
            }
        }).start();

        FileMessageConsumer bean = bean(FileMessageConsumer.class);
        await().until(() -> bean.list().size() == 10);
        assertThat(bean.list()).allSatisfy(s -> assertThat(s).startsWith("file-"));
    }

    @Test
    public void testPriceProducer() {
        prices.toFile().mkdirs();

        addClasses(PriceProducer.class);
        addConfig(getProducerConfig());
        initialize();

        await().until(() -> prices.toFile().listFiles().length >= 10);

    }

    @Test
    public void testPriceMessageProducer() {
        prices.toFile().mkdirs();

        addClasses(PriceMessageProducer.class);
        addConfig(getProducerConfig());
        initialize();

        await().until(() -> prices.toFile().listFiles().length >= 10);

    }

    private MapBasedConfig getConsumerConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("mp.messaging.incoming.files.connector", "smallrye-camel");
        config.put("mp.messaging.incoming.files.endpoint-uri", "file:target/orders/?delete=true&charset=utf-8");

        return new MapBasedConfig(config);
    }

    private MapBasedConfig getProducerConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("mp.messaging.outgoing.prices.connector", "smallrye-camel");
        config.put("mp.messaging.outgoing.prices.endpoint-uri",
                "file:target/prices/?fileName=${date:now:yyyyMMddssSS}.txt&charset=utf-8");
        return new MapBasedConfig(config);
    }

}
