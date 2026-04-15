package io.smallrye.reactive.messaging.recipes;

import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.ChangeType;

/**
 * Recipe for migrating Vert.x VertxInternal from impl to internal package.
 * Migrates io.vertx.core.impl.VertxInternal to io.vertx.core.internal.VertxInternal.
 */
public class VertxInternalMigration extends Recipe {

    @Override
    public String getDisplayName() {
        return "Migrate Vert.x VertxInternal package";
    }

    @Override
    public String getDescription() {
        return "Migrates io.vertx.core.impl.VertxInternal to io.vertx.core.internal.VertxInternal " +
                "for Vert.x 5 compatibility.";
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new ChangeType(
                "io.vertx.core.impl.VertxInternal",
                "io.vertx.core.internal.VertxInternal",
                false).getVisitor();
    }
}
