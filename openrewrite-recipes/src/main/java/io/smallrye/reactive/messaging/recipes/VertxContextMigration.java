package io.smallrye.reactive.messaging.recipes;

import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.ChangeType;

/**
 * Recipe for migrating Vert.x ContextInternal from impl to internal package.
 * Migrates io.vertx.core.impl.ContextInternal to io.vertx.core.internal.ContextInternal.
 */
public class VertxContextMigration extends Recipe {

    @Override
    public String getDisplayName() {
        return "Migrate Vert.x ContextInternal package";
    }

    @Override
    public String getDescription() {
        return "Migrates io.vertx.core.impl.ContextInternal to io.vertx.core.internal.ContextInternal " +
                "for Vert.x 5 compatibility.";
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new ChangeType(
                "io.vertx.core.impl.ContextInternal",
                "io.vertx.core.internal.ContextInternal",
                false).getVisitor();
    }
}
