package io.smallrye.reactive.messaging.recipes;

import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.ChangeMethodName;

/**
 * Recipe for migrating WorkerExecutor.getPool() to pool() for Vert.x 5.
 * Migrates io.vertx.core.WorkerExecutor.getPool() to io.vertx.core.WorkerExecutor.pool().
 */
public class MigrateWorkerExecutorGetPool extends Recipe {

    @Override
    public String getDisplayName() {
        return "Migrate WorkerExecutor.getPool() to pool()";
    }

    @Override
    public String getDescription() {
        return "Migrates io.vertx.core.WorkerExecutor.getPool() to pool() for Vert.x 5 compatibility.";
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new ChangeMethodName(
                "io.vertx.core.WorkerExecutor getPool()",
                "pool",
                false,
                null).getVisitor();
    }
}
