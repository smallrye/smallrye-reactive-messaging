package io.smallrye.reactive.messaging.recipes;

import static org.openrewrite.java.Assertions.java;

import org.junit.jupiter.api.Test;
import org.openrewrite.java.JavaParser;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

/**
 * Test for the Vert.x internal packages migration recipe.
 * Tests migration of impl package to internal package for various Vert.x types.
 */
class VertxInternalPackagesMigrationTest implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipeFromYaml(
                """
                        ---
                        type: specs.openrewrite.org/v1beta/recipe
                        name: io.smallrye.reactive.messaging.recipes.MigrateVertxInternalPackages
                        displayName: Migrate Vert.x Internal Package Relocations
                        description: Migrates Vert.x internal packages from impl to internal.
                        recipeList:
                          - org.openrewrite.java.ChangeType:
                              oldFullyQualifiedTypeName: io.vertx.core.impl.ContextInternal
                              newFullyQualifiedTypeName: io.vertx.core.internal.ContextInternal
                          - org.openrewrite.java.ChangeType:
                              oldFullyQualifiedTypeName: io.vertx.core.impl.VertxInternal
                              newFullyQualifiedTypeName: io.vertx.core.internal.VertxInternal
                          - org.openrewrite.java.ChangeType:
                              oldFullyQualifiedTypeName: io.vertx.core.impl.WorkerExecutor
                              newFullyQualifiedTypeName: io.vertx.core.internal.WorkerExecutor
                        """,
                "io.smallrye.reactive.messaging.recipes.MigrateVertxInternalPackages")
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
                                """,
                        """
                                package io.vertx.core.impl;
                                public interface VertxInternal {
                                    void deployVerticle(String name);
                                }
                                """,
                        """
                                package io.vertx.core.internal;
                                public interface VertxInternal {
                                    void deployVerticle(String name);
                                }
                                """,
                        """
                                package io.vertx.core.impl;
                                public interface WorkerExecutor {
                                    void executeBlocking(Runnable task);
                                }
                                """,
                        """
                                package io.vertx.core.internal;
                                public interface WorkerExecutor {
                                    void executeBlocking(Runnable task);
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
                                        ctx.runOnContext(() -> {});
                                    }
                                }
                                """,
                        """
                                import io.vertx.core.internal.ContextInternal;

                                class MyClass {
                                    private ContextInternal context;

                                    void execute(ContextInternal ctx) {
                                        ctx.runOnContext(() -> {});
                                    }
                                }
                                """));
    }

    @Test
    void migrateVertxInternalImport() {
        rewriteRun(
                java(
                        """
                                import io.vertx.core.impl.VertxInternal;

                                class MyService {
                                    private VertxInternal vertx;

                                    void deploy(VertxInternal v) {
                                        v.deployVerticle("MyVerticle");
                                    }
                                }
                                """,
                        """
                                import io.vertx.core.internal.VertxInternal;

                                class MyService {
                                    private VertxInternal vertx;

                                    void deploy(VertxInternal v) {
                                        v.deployVerticle("MyVerticle");
                                    }
                                }
                                """));
    }

    @Test
    void migrateWorkerExecutorImport() {
        rewriteRun(
                java(
                        """
                                import io.vertx.core.impl.WorkerExecutor;

                                class WorkerService {
                                    private WorkerExecutor executor;

                                    void executeTask(WorkerExecutor worker) {
                                        worker.executeBlocking(() -> {
                                            // heavy task
                                        });
                                    }
                                }
                                """,
                        """
                                import io.vertx.core.internal.WorkerExecutor;

                                class WorkerService {
                                    private WorkerExecutor executor;

                                    void executeTask(WorkerExecutor worker) {
                                        worker.executeBlocking(() -> {
                                            // heavy task
                                        });
                                    }
                                }
                                """));
    }

    @Test
    void migrateMultipleImports() {
        rewriteRun(
                java(
                        """
                                import io.vertx.core.impl.ContextInternal;
                                import io.vertx.core.impl.VertxInternal;
                                import io.vertx.core.impl.WorkerExecutor;

                                class ComplexService {
                                    private ContextInternal context;
                                    private VertxInternal vertx;
                                    private WorkerExecutor executor;

                                    void process(ContextInternal ctx, VertxInternal v, WorkerExecutor w) {
                                        ctx.runOnContext(() -> {
                                            v.deployVerticle("Test");
                                            w.executeBlocking(() -> {});
                                        });
                                    }
                                }
                                """,
                        """
                                import io.vertx.core.internal.ContextInternal;
                                import io.vertx.core.internal.VertxInternal;
                                import io.vertx.core.internal.WorkerExecutor;

                                class ComplexService {
                                    private ContextInternal context;
                                    private VertxInternal vertx;
                                    private WorkerExecutor executor;

                                    void process(ContextInternal ctx, VertxInternal v, WorkerExecutor w) {
                                        ctx.runOnContext(() -> {
                                            v.deployVerticle("Test");
                                            w.executeBlocking(() -> {});
                                        });
                                    }
                                }
                                """));
    }

    @Test
    void migrateFullyQualifiedNames() {
        rewriteRun(
                java(
                        """
                                class MyClass {
                                    private io.vertx.core.impl.ContextInternal context;
                                    private io.vertx.core.impl.VertxInternal vertx;
                                    private io.vertx.core.impl.WorkerExecutor executor;

                                    void execute() {
                                        io.vertx.core.impl.ContextInternal ctx = this.context;
                                    }
                                }
                                """,
                        """
                                import io.vertx.core.internal.ContextInternal;
                                import io.vertx.core.internal.VertxInternal;
                                import io.vertx.core.internal.WorkerExecutor;

                                class MyClass {
                                    private ContextInternal context;
                                    private VertxInternal vertx;
                                    private WorkerExecutor executor;

                                    void execute() {
                                        ContextInternal ctx = this.context;
                                    }
                                }
                                """));
    }

    @Test
    void migrateCastAndInstanceOf() {
        rewriteRun(
                java(
                        """
                                import io.vertx.core.impl.VertxInternal;

                                class MyService {
                                    void process(Object obj) {
                                        if (obj instanceof VertxInternal) {
                                            VertxInternal vertx = (VertxInternal) obj;
                                            vertx.deployVerticle("Test");
                                        }
                                    }
                                }
                                """,
                        """
                                import io.vertx.core.internal.VertxInternal;

                                class MyService {
                                    void process(Object obj) {
                                        if (obj instanceof VertxInternal) {
                                            VertxInternal vertx = (VertxInternal) obj;
                                            vertx.deployVerticle("Test");
                                        }
                                    }
                                }
                                """));
    }
}
