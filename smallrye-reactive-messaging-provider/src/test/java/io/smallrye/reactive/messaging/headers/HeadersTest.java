package io.smallrye.reactive.messaging.headers;

import org.eclipse.microprofile.reactive.messaging.Headers;
import org.junit.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.*;

public class HeadersTest {

    @Test
    public void testEmptyHeaders() {
        Headers headers = Headers.empty();
        assertThat(headers).hasSize(0)
            .doesNotContainKeys("foo")
            .doesNotContainValue("bar");

        assertThat(headers.getAsBoolean("missing")).isFalse();
        assertThat(headers.getAsBoolean("missing", true)).isTrue();
        assertThat(headers.getAsInteger("missing", 23)).isEqualTo(23);
        assertThat(headers.getAsLong("missing", 23L)).isEqualTo(23L);
        assertThat(headers.getAsString("missing", "hello")).isEqualTo("hello");
        assertThat(headers.getAsDouble("missing", 23.3)).isEqualTo(23.3);
        assertThat(headers.containsValue(null)).isFalse();
    }

    @Test
    public void testCreationFromEmpty() {
        Headers headers = Headers.of();
        assertThat(headers).hasSize(0)
            .doesNotContainKeys("foo")
            .doesNotContainValue("bar");

        Headers headers2 = headers.with("foo", "bar");
        assertThat(headers2).hasSize(1)
            .containsKey("foo")
            .containsValue("bar");
        assertThat(headers).isNotSameAs(headers2);
    }

    @Test
    public void testOf() {
        Headers h1 = Headers.of();
        assertThat(h1).isEmpty();

        Headers h2 = Headers.of("a", 12);
        assertThat(h2).hasSize(1).containsKey("a").containsValue(12);
        assertThat(h2.getAsInteger("a", -1)).isEqualTo(12);

        Headers h3 = Headers.of("a", 12, "b", true);
        assertThat(h3).hasSize(2).containsKeys("a", "b").containsValues(12, true);
        assertThat(h3.getAsInteger("a", -1)).isEqualTo(12);
        assertThat(h3.getAsBoolean("b")).isTrue();

        Headers h4 = Headers.of("a", 12, "b", true, "c", 23.3);
        assertThat(h4).hasSize(3).containsKeys("a", "b", "c").containsValues(12, true, 23.3);
        assertThat(h4.getAsInteger("a", -1)).isEqualTo(12);
        assertThat(h4.getAsBoolean("b")).isTrue();
        assertThat(h4.getAsDouble("c", -1)).isEqualTo(23.3);

        Headers h5 = Headers.of("a", 12, "b", true, "c", 23.3, "d", Long.MAX_VALUE);
        assertThat(h5).hasSize(4).containsKeys("a", "b", "c", "d").containsValues(12, true, 23.3, Long.MAX_VALUE);
        assertThat(h5.getAsInteger("a", -1)).isEqualTo(12);
        assertThat(h5.getAsBoolean("b")).isTrue();
        assertThat(h5.getAsDouble("c", -1)).isEqualTo(23.3);
        assertThat(h5.getAsLong("d", -1)).isEqualTo(Long.MAX_VALUE);

        Headers h6 = Headers.of("a", 12, "b", true, "c", 23.3, "d", Long.MAX_VALUE, "e", "hello");
        assertThat(h6).hasSize(5).containsKeys("a", "b", "c", "d").containsValues(12, true, 23.3, Long.MAX_VALUE);
        assertThat(h6.getAsInteger("a", -1)).isEqualTo(12);
        assertThat(h6.getAsBoolean("b")).isTrue();
        assertThat(h6.getAsDouble("c", -1)).isEqualTo(23.3);
        assertThat(h6.getAsLong("d", -1)).isEqualTo(Long.MAX_VALUE);
        assertThat(h6.getAsString("e", null)).isEqualTo("hello");
    }

    @Test
    public void testCreationWithOneEntry() {
        Headers headers = Headers.of("foo", "bar");
        assertThat(headers).hasSize(1)
            .containsKey("foo")
            .containsValue("bar");

        assertThat(headers.getAsString("foo", null)).isEqualTo("bar");
        assertThat(headers.containsKey("foo")).isTrue();
        assertThat(headers.containsValue("bar")).isTrue();
        List<Object> values = new ArrayList<>();
        headers.forEach((k, v) -> values.add(v));
        assertThat(values).containsExactly("bar");
        assertThat(headers.containsValue(null)).isFalse();
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    public void cannotCreateFromNull() {
        assertThatThrownBy(() -> Headers.of(null, "value")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Headers.of("key", null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Headers.of("k", "v", null, "v")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Headers.of("k", "v").containsKey(null)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testContainsWithNull() {
        Headers headers = Headers.of("a", "b");
        assertThatThrownBy(() -> headers.containsKey(null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> headers.containsKey((Object) null)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testGetWithNull() {
        Headers headers = Headers.of("a", "b");
        assertThatThrownBy(() -> headers.get(null)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testWithout() {
        Headers headers = Headers.empty().with("hello", "world");
        assertThat(headers).hasSize(1);
        Headers without = headers.without("hello");
        assertThat(headers).isNotSameAs(without);
        assertThat(without).isEmpty();
    }

    @Test
    public void testGetObject() {
        Person person = new Person();
        person.name = "mark";
        Headers headers = Headers.of("k1", person, "k2", new byte[] { 1, 2, 3, 4});

        Person p = headers.get("k1", new Person());
        assertThat(p).isNotNull();
        assertThat(p.name).isEqualTo("mark");

        byte[] bytes = headers.get("k2", new byte[0]);
        assertThat(bytes).isNotNull().hasSize(4).containsExactly(1, 2, 3, 4);
    }

    @Test
    public void testFrom() {
        Headers headers = Headers.empty().with("hello", "world");
        Headers copy = Headers.from(headers).with("k", "v").with("hello", "monde").build();
        assertThat(copy).hasSize(2).contains(entry("k", "v"), entry("hello", "monde"));
        assertThat(headers).hasSize(1).containsExactly(entry("hello", "world"));
    }

    @Test
    public void testCopy() {
        Person person = new Person();
        person.name = "mark";

        Headers headers = Headers.builder()
            .with("foo", 1234)
            .with("person", person).build();

        assertThat(headers).hasSize(2);
        assertThat(headers.get("person", Person.class).name).isEqualTo("mark");

        Headers copy = headers.copy();
        assertThat(copy).hasSize(2);
        assertThat(copy.get("person", Person.class).name).isEqualTo("mark");
    }

    @Test
    public void testImmutability() {
        Headers headers = Headers.of("k", "v");
        assertThat(headers).hasSize(1);

        // Values
        Collection<Object> collection = headers.values();
        assertThat(collection).hasSize(1).containsExactly("v");
        assertThatThrownBy(() -> collection.add("something")).isInstanceOf(UnsupportedOperationException.class);
        assertThat(headers).hasSize(1).doesNotContainValue("something");

        // Keys
        Set<String> keys = headers.keySet();
        assertThat(keys).hasSize(1).containsExactly("k");
        assertThatThrownBy(() -> keys.add("something")).isInstanceOf(UnsupportedOperationException.class);
        assertThat(headers).hasSize(1).doesNotContainKey("something");

        // Entry
        Set<Map.Entry<String, Object>> entries = headers.entrySet();
        assertThat(entries).hasSize(1);
        entries.add(entry("foo", "bar"));
        assertThat(headers).hasSize(1).doesNotContainKey("foo");

        // Entry modification
        assertThatThrownBy(() -> {
            Map.Entry<String, Object> next = entries.iterator().next();
            next.setValue("foo");
        }).isInstanceOf(UnsupportedOperationException.class);
        assertThat(headers).hasSize(1).doesNotContainValue("foo").containsExactly(entry("k", "v"));

        // Modification methods
        assertThatThrownBy(() -> headers.put("foo", "bar")).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> headers.remove("foo")).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> headers.remove("foo", "bar")).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> headers.putAll(Collections.emptyMap())).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> headers.putIfAbsent("foo", "bar")).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> headers.compute("foo", (s, o) -> o)).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> headers.computeIfPresent("foo", (s, o) -> o)).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> headers.computeIfAbsent("foo", s -> "bat")).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> headers.replace("foo", "bat", "bar")).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> headers.replaceAll((s, o) -> o)).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> headers.merge("foo", "bar", (s, o) -> o)).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(headers::clear).isInstanceOf(UnsupportedOperationException.class);
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    public void testBuilderWithDuplicateKeys() {
        assertThatThrownBy(() -> Headers.of("a", 1, "a", 2)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testConversionFromString() {
        Headers headers = Headers.builder()
            .with("int", "1234")
            .with("long", "11111111")
            .with("boolean-t", "true")
            .with("boolean-f", "false")
            .with("double", "23.3")
            .with("person", new Person())
            .build();

        assertThat(headers.getAsInteger("int", -1)).isEqualTo(1234);
        assertThat(headers.getAsLong("long", -1)).isEqualTo(11111111L);
        assertThat(headers.getAsBoolean("boolean-t")).isTrue();
        assertThat(headers.getAsBoolean("boolean-f")).isFalse();
        assertThat(headers.getAsDouble("double", -1)).isEqualTo(23.3);
        assertThat(headers.getAsBoolean("int")).isFalse();

        assertThatThrownBy(() -> headers.getAsInteger("boolean-t", -1)).isInstanceOf(NumberFormatException.class);
        assertThatThrownBy(() -> headers.getAsLong("boolean-t", -1)).isInstanceOf(NumberFormatException.class);
        assertThatThrownBy(() -> headers.getAsDouble("boolean-t", -1)).isInstanceOf(NumberFormatException.class);

        assertThatThrownBy(() -> headers.getAsBoolean("person")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> headers.getAsInteger("person", -1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> headers.getAsLong("person", -1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> headers.getAsDouble("person", -1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> headers.get("person", String.class)).isInstanceOf(ClassCastException.class);

        assertThat(headers.getAsString("person", null)).isNotNull().contains("Person");
        assertThat(headers.get("missing", new Person())).isNotNull();
    }

    @Test
    public void testGetWithTarget() {
        Person p1 = new Person();
        p1.name = "mark";
        Headers headers = Headers.of("person", p1);
        assertThatThrownBy(() -> headers.get("person", null)).isInstanceOf(IllegalArgumentException.class);
        assertThat(headers.get("missing", Person.class)).isNull();
        assertThat(headers.get("person", Person.class)).isNotNull().isEqualTo(p1);
    }

    private class Person {
        String name;
    }

}
