package io.smallrye.reactive.messaging.recipes;

import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.ChangeType;

/**
 * Recipe for migrating Vert.x WorkerExecutor from impl to internal package.
 * Migrates io.vertx.core.impl.WorkerExecutor to io.vertx.core.internal.WorkerExecutor.
 */
public class WorkerExecutorMigration extends Recipe {

    @Override
    public String getDisplayName() {
        return "Migrate Vert.x WorkerExecutor package";
    }

    @Override
    public String getDescription() {
        return "Migrates io.vertx.core.impl.WorkerExecutor to io.vertx.core.internal.WorkerExecutor " +
                "for Vert.x 5 compatibility.";
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new ChangeType(
                "io.vertx.core.impl.WorkerExecutor",
                "io.vertx.core.internal.WorkerExecutor",
                false).getVisitor();
    }
}
