package io.smallrye.reactive.messaging.recipes;

import static org.openrewrite.java.Assertions.java;

import org.junit.jupiter.api.Test;
import org.openrewrite.java.JavaParser;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

/**
 * Test for the Vert.x ContextInternal migration recipe.
 */
class VertxContextMigrationTest implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipe(new VertxContextMigration())
                .parser(JavaParser.fromJavaVersion().dependsOn(
                        """
                                package io.vertx.core.impl;
                                public interface ContextInternal {
                                    void runOnContext(Runnable action);
                                }
                                """,
                        """
                                package io.vertx.core.internal;
                                public interface ContextInternal {
                                    void runOnContext(Runnable action);
                                }
                                """));
    }

    @Test
    void migrateContextInternalImport() {
        rewriteRun(
                java(
                        """
                                import io.vertx.core.impl.ContextInternal;

                                class MyClass {
                                    private ContextInternal context;

                                    void execute(ContextInternal ctx) {
                                        ctx.runOnContext(() -> {
                                            // do something
                                        });
                                    }
                                }
                                """,
                        """
                                import io.vertx.core.internal.ContextInternal;

                                class MyClass {
                                    private ContextInternal context;

                                    void execute(ContextInternal ctx) {
                                        ctx.runOnContext(() -> {
                                            // do something
                                        });
                                    }
                                }
                                """));
    }

    @Test
    void migrateContextInternalFullyQualified() {
        rewriteRun(
                java(
                        """
                                class MyClass {
                                    private io.vertx.core.impl.ContextInternal context;

                                    void execute(io.vertx.core.impl.ContextInternal ctx) {
                                        ctx.runOnContext(() -> {
                                            // do something
                                        });
                                    }
                                }
                                """,
                        """
                                import io.vertx.core.internal.ContextInternal;

                                class MyClass {
                                    private ContextInternal context;

                                    void execute(ContextInternal ctx) {
                                        ctx.runOnContext(() -> {
                                            // do something
                                        });
                                    }
                                }
                                """));
    }

    @Test
    void migrateContextInternalCast() {
        rewriteRun(
                java(
                        """
                                import io.vertx.core.impl.ContextInternal;

                                class MyClass {
                                    void execute(Object obj) {
                                        if (obj instanceof ContextInternal) {
                                            ContextInternal ctx = (ContextInternal) obj;
                                            ctx.runOnContext(() -> {});
                                        }
                                    }
                                }
                                """,
                        """
                                import io.vertx.core.internal.ContextInternal;

                                class MyClass {
                                    void execute(Object obj) {
                                        if (obj instanceof ContextInternal) {
                                            ContextInternal ctx = (ContextInternal) obj;
                                            ctx.runOnContext(() -> {});
                                        }
                                    }
                                }
                                """));
    }
}
