package io.smallrye.reactive.messaging.inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import io.smallrye.reactive.messaging.providers.extension.TupleKeyValueExtractor;

public class TableInjectionTest extends WeldTestBaseWithoutTails {

    @Test
    public void testInjectionOfTable() {
        addBeanClass(SourceBean.class, PayloadKeyValueExtractor.class, TupleKeyValueExtractor.class);
        BeanInjectedWithATable bean = installInitializeAndGet(BeanInjectedWithATable.class);
        assertThat(bean.getMapBonjour()).containsValues("B", "O", "N", "J", "U", "R");
        List<String> keysConsumer1 = new CopyOnWriteArrayList<>();
        List<String> keysConsumer2 = new CopyOnWriteArrayList<>();
        bean.getHello().subscribe().with(t -> keysConsumer1.add(t.getItem1()));
        bean.getHello().subscribe().with(t -> keysConsumer2.add(t.getItem1()));
        MutinyEmitter<String> emitter = bean.getHelloEmitter();
        emitter.send("h").await().indefinitely();
        emitter.send("e").await().indefinitely();
        emitter.send("l").await().indefinitely();
        await().untilAsserted(() -> assertThat(bean.getHello().toMap()).containsKeys("h", "e", "l"));
        await().untilAsserted(() -> assertThat(keysConsumer1).contains("h", "e", "l"));
        await().untilAsserted(() -> assertThat(keysConsumer2).contains("h", "e", "l"));
        await().untilAsserted(() -> assertThat(keysConsumer2).contains("h", "e", "l"));

        Map<String, String> mapConsumer = new HashMap<>();
        bean.getHello().toMapStream().subscribe().with(m -> mapConsumer.putAll(m));

        emitter.send("l").await().indefinitely();
        emitter.send("o").await().indefinitely();

        await().untilAsserted(() -> assertThat(mapConsumer).containsKeys("h", "e", "l", "o"));

        await().untilAsserted(() -> assertThat(bean.getHello().toMap()).containsKeys("h", "e", "l", "o"));
        await().untilAsserted(() -> assertThat(keysConsumer1).contains("h", "e", "l", "o"));
        await().untilAsserted(() -> assertThat(keysConsumer2).contains("h", "e", "l", "o"));
        await().untilAsserted(() -> assertThat(keysConsumer2).contains("h", "e", "l", "o"));

    }

    @Test
    public void testTable() {
        addBeanClass(SourceBean.class, PayloadKeyValueExtractor.class, TupleKeyValueExtractor.class);
        BeanInjectedWithATable bean = installInitializeAndGet(BeanInjectedWithATable.class);
        await().untilAsserted(() -> assertThat(bean.getBonjour().toMap()).containsKeys("B", "O", "N", "J", "U", "R"));
        MutinyEmitter<String> emitter = bean.getHelloEmitter();
        emitter.send("h").await().indefinitely();
        emitter.send("e").await().indefinitely();
        emitter.send("l").await().indefinitely();
        emitter.send("l").await().indefinitely();
        emitter.send("o").await().indefinitely();
        await().untilAsserted(() -> assertThat(bean.getHello().toMap()).containsKeys("h", "e", "l", "o"));
    }

}
