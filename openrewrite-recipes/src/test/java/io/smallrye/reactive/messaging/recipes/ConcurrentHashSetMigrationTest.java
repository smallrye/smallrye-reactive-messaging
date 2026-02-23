package io.smallrye.reactive.messaging.recipes;

import static org.openrewrite.java.Assertions.java;

import org.junit.jupiter.api.Test;
import org.openrewrite.java.JavaParser;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

/**
 * Test for the ConcurrentHashSet to CopyOnWriteArraySet migration recipe.
 */
class ConcurrentHashSetMigrationTest implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipeFromYaml(
                """
                        ---
                        type: specs.openrewrite.org/v1beta/recipe
                        name: io.smallrye.reactive.messaging.recipes.MigrateRemovedVertxClasses
                        displayName: Migrate Removed Vert.x Classes
                        description: Migrates Vert.x classes that were removed in version 5 to their replacements.
                        recipeList:
                          - org.openrewrite.java.ChangeType:
                              oldFullyQualifiedTypeName: io.vertx.core.impl.ConcurrentHashSet
                              newFullyQualifiedTypeName: java.util.concurrent.CopyOnWriteArraySet
                        """,
                "io.smallrye.reactive.messaging.recipes.MigrateRemovedVertxClasses")
                .parser(JavaParser.fromJavaVersion().dependsOn(
                        """
                                package io.vertx.core.impl;
                                import java.util.Set;
                                public class ConcurrentHashSet<E> implements Set<E> {
                                    public ConcurrentHashSet() {}
                                    public boolean add(E e) { return false; }
                                    public boolean remove(Object o) { return false; }
                                    public int size() { return 0; }
                                    public boolean isEmpty() { return true; }
                                    public boolean contains(Object o) { return false; }
                                    public java.util.Iterator<E> iterator() { return null; }
                                    public Object[] toArray() { return new Object[0]; }
                                    public <T> T[] toArray(T[] a) { return a; }
                                    public boolean containsAll(java.util.Collection<?> c) { return false; }
                                    public boolean addAll(java.util.Collection<? extends E> c) { return false; }
                                    public boolean retainAll(java.util.Collection<?> c) { return false; }
                                    public boolean removeAll(java.util.Collection<?> c) { return false; }
                                    public void clear() {}
                                }
                                """));
    }

    @Test
    void migrateConcurrentHashSetImport() {
        rewriteRun(
                java(
                        """
                                import io.vertx.core.impl.ConcurrentHashSet;

                                class MyService {
                                    private ConcurrentHashSet<String> items = new ConcurrentHashSet<>();

                                    void addItem(String item) {
                                        items.add(item);
                                    }
                                }
                                """,
                        """
                                import java.util.concurrent.CopyOnWriteArraySet;

                                class MyService {
                                    private CopyOnWriteArraySet<String> items = new CopyOnWriteArraySet<>();

                                    void addItem(String item) {
                                        items.add(item);
                                    }
                                }
                                """));
    }

    @Test
    void migrateConcurrentHashSetFullyQualified() {
        rewriteRun(
                java(
                        """
                                class MyService {
                                    private io.vertx.core.impl.ConcurrentHashSet<Integer> numbers;

                                    MyService() {
                                        numbers = new io.vertx.core.impl.ConcurrentHashSet<>();
                                    }
                                }
                                """,
                        """
                                import java.util.concurrent.CopyOnWriteArraySet;

                                class MyService {
                                    private CopyOnWriteArraySet<Integer> numbers;

                                    MyService() {
                                        numbers = new CopyOnWriteArraySet<>();
                                    }
                                }
                                """));
    }

    @Test
    void migrateConcurrentHashSetAsParameter() {
        rewriteRun(
                java(
                        """
                                import io.vertx.core.impl.ConcurrentHashSet;

                                class MyService {
                                    void processItems(ConcurrentHashSet<String> items) {
                                        for (String item : items) {
                                            System.out.println(item);
                                        }
                                    }

                                    ConcurrentHashSet<String> getItems() {
                                        return new ConcurrentHashSet<>();
                                    }
                                }
                                """,
                        """
                                import java.util.concurrent.CopyOnWriteArraySet;

                                class MyService {
                                    void processItems(CopyOnWriteArraySet<String> items) {
                                        for (String item : items) {
                                            System.out.println(item);
                                        }
                                    }

                                    CopyOnWriteArraySet<String> getItems() {
                                        return new CopyOnWriteArraySet<>();
                                    }
                                }
                                """));
    }

    @Test
    void migrateConcurrentHashSetWithGenericTypes() {
        rewriteRun(
                java(
                        """
                                import io.vertx.core.impl.ConcurrentHashSet;
                                import java.util.List;

                                class MyService {
                                    private ConcurrentHashSet<List<String>> nestedItems = new ConcurrentHashSet<>();

                                    void addList(List<String> list) {
                                        nestedItems.add(list);
                                    }
                                }
                                """,
                        """
                                import java.util.List;
                                import java.util.concurrent.CopyOnWriteArraySet;

                                class MyService {
                                    private CopyOnWriteArraySet<List<String>> nestedItems = new CopyOnWriteArraySet<>();

                                    void addList(List<String> list) {
                                        nestedItems.add(list);
                                    }
                                }
                                """));
    }

    @Test
    void migrateConcurrentHashSetInCollection() {
        rewriteRun(
                java(
                        """
                                import io.vertx.core.impl.ConcurrentHashSet;
                                import java.util.Set;

                                class MyService {
                                    private ConcurrentHashSet<String> items1 = new ConcurrentHashSet<>();
                                    private ConcurrentHashSet<Integer> items2 = new ConcurrentHashSet<>();

                                    void useAsSet(Set<String> set) {
                                        set.add("test");
                                    }

                                    void testMethod() {
                                        useAsSet(items1);
                                    }
                                }
                                """,
                        """
                                import java.util.Set;
                                import java.util.concurrent.CopyOnWriteArraySet;

                                class MyService {
                                    private CopyOnWriteArraySet<String> items1 = new CopyOnWriteArraySet<>();
                                    private CopyOnWriteArraySet<Integer> items2 = new CopyOnWriteArraySet<>();

                                    void useAsSet(Set<String> set) {
                                        set.add("test");
                                    }

                                    void testMethod() {
                                        useAsSet(items1);
                                    }
                                }
                                """));
    }

    @Test
    void migrateConcurrentHashSetWithInstanceOf() {
        rewriteRun(
                java(
                        """
                                import io.vertx.core.impl.ConcurrentHashSet;

                                class MyService {
                                    void checkType(Object obj) {
                                        if (obj instanceof ConcurrentHashSet) {
                                            ConcurrentHashSet<?> set = (ConcurrentHashSet<?>) obj;
                                            System.out.println(set.size());
                                        }
                                    }
                                }
                                """,
                        """
                                import java.util.concurrent.CopyOnWriteArraySet;

                                class MyService {
                                    void checkType(Object obj) {
                                        if (obj instanceof CopyOnWriteArraySet) {
                                            CopyOnWriteArraySet<?> set = (CopyOnWriteArraySet<?>) obj;
                                            System.out.println(set.size());
                                        }
                                    }
                                }
                                """));
    }

    @Test
    void migrateConcurrentHashSetVariableDeclaration() {
        rewriteRun(
                java(
                        """
                                import io.vertx.core.impl.ConcurrentHashSet;

                                class MyService {
                                    void createAndUse() {
                                        ConcurrentHashSet<String> tempSet = new ConcurrentHashSet<>();
                                        tempSet.add("item1");
                                        tempSet.add("item2");

                                        ConcurrentHashSet<String> anotherSet;
                                        anotherSet = tempSet;
                                    }
                                }
                                """,
                        """
                                import java.util.concurrent.CopyOnWriteArraySet;

                                class MyService {
                                    void createAndUse() {
                                        CopyOnWriteArraySet<String> tempSet = new CopyOnWriteArraySet<>();
                                        tempSet.add("item1");
                                        tempSet.add("item2");

                                        CopyOnWriteArraySet<String> anotherSet;
                                        anotherSet = tempSet;
                                    }
                                }
                                """));
    }
}
