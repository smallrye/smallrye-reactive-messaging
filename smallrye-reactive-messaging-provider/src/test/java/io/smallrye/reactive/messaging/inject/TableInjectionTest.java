package io.smallrye.reactive.messaging.inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.Table;
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

    @Test
    void testFilterTable() {
        addBeanClass(SourceBean.class, PayloadKeyValueExtractor.class, TupleKeyValueExtractor.class);
        BeanInjectedWithATable bean = installInitializeAndGet(BeanInjectedWithATable.class);
        Table<String, String> lTable = bean.getHello().filterKey("l");
        List<String> lKeys = new CopyOnWriteArrayList<>();
        lTable.subscribe().with(t -> lKeys.add(t.getItem1()));
        MutinyEmitter<String> emitter = bean.getHelloEmitter();
        emitter.send("h").await().indefinitely();
        emitter.send("e").await().indefinitely();
        emitter.send("l").await().indefinitely();
        emitter.send("l").await().indefinitely();
        emitter.send("o").await().indefinitely();
        await().untilAsserted(() -> assertThat(lKeys).containsOnly("l", "l"));
        await().untilAsserted(() -> assertThat(lTable.toMap()).containsOnlyKeys("l"));
    }

    @Test
    void testMapTable() {
        addBeanClass(SourceBean.class, PayloadKeyValueExtractor.class, TupleKeyValueExtractor.class);
        BeanInjectedWithATable bean = installInitializeAndGet(BeanInjectedWithATable.class);
        Table<String, String> vTable = bean.getHello().map((k, v) -> "value-" + v);
        List<String> values = new CopyOnWriteArrayList<>();
        vTable.subscribe().with(t -> values.add(t.getItem2()));
        MutinyEmitter<String> emitter = bean.getHelloEmitter();
        emitter.send("h").await().indefinitely();
        emitter.send("e").await().indefinitely();
        emitter.send("l").await().indefinitely();
        emitter.send("l").await().indefinitely();
        emitter.send("o").await().indefinitely();
        await().untilAsserted(() -> assertThat(values).containsOnly("value-h", "value-e", "value-l", "value-l", "value-o"));
        await().untilAsserted(() -> assertThat(vTable.toMap()).containsOnlyKeys("h", "e", "l", "o"));
    }

    @Test
    void testMapKeyTable() {
        addBeanClass(SourceBean.class, PayloadKeyValueExtractor.class, TupleKeyValueExtractor.class);
        BeanInjectedWithATable bean = installInitializeAndGet(BeanInjectedWithATable.class);
        Table<String, String> vTable = bean.getHello().mapKey((k, v) -> "k-" + v);
        List<String> values = new CopyOnWriteArrayList<>();
        vTable.subscribe().with(t -> values.add(t.getItem1()));
        MutinyEmitter<String> emitter = bean.getHelloEmitter();
        emitter.send("h").await().indefinitely();
        emitter.send("e").await().indefinitely();
        emitter.send("l").await().indefinitely();
        emitter.send("l").await().indefinitely();
        emitter.send("o").await().indefinitely();
        await().untilAsserted(() -> assertThat(values).containsOnly("k-h", "k-e", "k-l", "k-l", "k-o"));
        await().untilAsserted(() -> assertThat(vTable.toMap()).containsOnlyKeys("k-h", "k-e", "k-l", "k-o"));
    }

    @Test
    void testWithEmitOnChange() {
        addBeanClass(SourceBean.class, PayloadKeyValueExtractor.class, TupleKeyValueExtractor.class);
        BeanInjectedWithATable bean = installInitializeAndGet(BeanInjectedWithATable.class);
        Table<String, String> hello = bean.getHello().withEmitOnChange();
        List<String> values = new CopyOnWriteArrayList<>();
        hello.subscribe().with(t -> values.add(t.getItem2()));
        MutinyEmitter<String> emitter = bean.getHelloEmitter();
        emitter.send("h").await().indefinitely();
        emitter.send("e").await().indefinitely();
        emitter.send("l").await().indefinitely();
        emitter.send("l").await().indefinitely();
        emitter.send("o").await().indefinitely();
        await().untilAsserted(() -> assertThat(values).containsOnly("h", "e", "l", "o"));
        await().untilAsserted(() -> assertThat(hello.toMap()).containsOnlyKeys("h", "e", "l", "o"));
    }
}
