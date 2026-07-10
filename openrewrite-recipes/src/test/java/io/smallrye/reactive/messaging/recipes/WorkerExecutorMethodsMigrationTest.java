package io.smallrye.reactive.messaging.recipes;

import static org.openrewrite.java.Assertions.java;

import org.junit.jupiter.api.Test;
import org.openrewrite.java.JavaParser;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

/**
 * Test for the WorkerExecutor method renames migration recipe.
 */
class WorkerExecutorMethodsMigrationTest implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipeFromYaml(
                """
                        ---
                        type: specs.openrewrite.org/v1beta/recipe
                        name: io.smallrye.reactive.messaging.recipes.MigrateWorkerExecutorMethods
                        displayName: Migrate Vert.x WorkerExecutor Method Renames
                        description: Migrates renamed methods on WorkerExecutor for Vert.x 5 compatibility.
                        recipeList:
                          - org.openrewrite.java.ChangeMethodName:
                              methodPattern: io.vertx.core.WorkerExecutor getPool()
                              newMethodName: pool
                        """,
                "io.smallrye.reactive.messaging.recipes.MigrateWorkerExecutorMethods")
                .parser(JavaParser.fromJavaVersion().dependsOn(
                        """
                                package io.vertx.core;
                                public interface WorkerExecutor {
                                    Object getPool();
                                    Object pool();
                                    void executeBlocking(Runnable task);
                                }
                                """));
    }

    @Test
    void migrateGetPoolToPool() {
        rewriteRun(
                java(
                        """
                                import io.vertx.core.WorkerExecutor;

                                class MyService {
                                    void useWorkerExecutor(WorkerExecutor executor) {
                                        Object pool = executor.getPool();
                                        System.out.println(pool);
                                    }
                                }
                                """,
                        """
                                import io.vertx.core.WorkerExecutor;

                                class MyService {
                                    void useWorkerExecutor(WorkerExecutor executor) {
                                        Object pool = executor.pool();
                                        System.out.println(pool);
                                    }
                                }
                                """));
    }

    @Test
    void migrateGetPoolInChain() {
        rewriteRun(
                java(
                        """
                                import io.vertx.core.WorkerExecutor;

                                class MyService {
                                    void printPool(WorkerExecutor executor) {
                                        System.out.println(executor.getPool().toString());
                                    }
                                }
                                """,
                        """
                                import io.vertx.core.WorkerExecutor;

                                class MyService {
                                    void printPool(WorkerExecutor executor) {
                                        System.out.println(executor.pool().toString());
                                    }
                                }
                                """));
    }

    @Test
    void migrateMultipleGetPoolCalls() {
        rewriteRun(
                java(
                        """
                                import io.vertx.core.WorkerExecutor;

                                class MyService {
                                    private WorkerExecutor executor;

                                    void comparePoolsMethod1(WorkerExecutor other) {
                                        Object pool1 = executor.getPool();
                                        Object pool2 = other.getPool();
                                        boolean same = pool1 == pool2;
                                    }

                                    Object getPoolWrapper() {
                                        return executor.getPool();
                                    }
                                }
                                """,
                        """
                                import io.vertx.core.WorkerExecutor;

                                class MyService {
                                    private WorkerExecutor executor;

                                    void comparePoolsMethod1(WorkerExecutor other) {
                                        Object pool1 = executor.pool();
                                        Object pool2 = other.pool();
                                        boolean same = pool1 == pool2;
                                    }

                                    Object getPoolWrapper() {
                                        return executor.pool();
                                    }
                                }
                                """));
    }

    @Test
    void doesNotChangeOtherMethods() {
        rewriteRun(
                java(
                        """
                                import io.vertx.core.WorkerExecutor;

                                class MyService {
                                    void execute(WorkerExecutor executor) {
                                        executor.executeBlocking(() -> {
                                            System.out.println("blocking task");
                                        });
                                    }
                                }
                                """));
    }

    @Test
    void migrateGetPoolWithFieldAccess() {
        rewriteRun(
                java(
                        """
                                import io.vertx.core.WorkerExecutor;

                                class MyService {
                                    private WorkerExecutor executor;

                                    void usePool() {
                                        Object pool = this.executor.getPool();
                                    }
                                }
                                """,
                        """
                                import io.vertx.core.WorkerExecutor;

                                class MyService {
                                    private WorkerExecutor executor;

                                    void usePool() {
                                        Object pool = this.executor.pool();
                                    }
                                }
                                """));
    }
}
