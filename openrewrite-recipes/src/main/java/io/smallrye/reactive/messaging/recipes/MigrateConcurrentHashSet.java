package io.smallrye.reactive.messaging.recipes;

import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.ChangeType;

/**
 * Recipe for migrating ConcurrentHashSet to CopyOnWriteArraySet.
 * Migrates io.vertx.core.impl.ConcurrentHashSet to java.util.concurrent.CopyOnWriteArraySet
 * as ConcurrentHashSet was removed in Vert.x 5.
 */
public class MigrateConcurrentHashSet extends Recipe {

    @Override
    public String getDisplayName() {
        return "Migrate ConcurrentHashSet to CopyOnWriteArraySet";
    }

    @Override
    public String getDescription() {
        return "Migrates io.vertx.core.impl.ConcurrentHashSet to java.util.concurrent.CopyOnWriteArraySet " +
                "as ConcurrentHashSet was removed in Vert.x 5.";
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new ChangeType(
                "io.vertx.core.impl.ConcurrentHashSet",
                "java.util.concurrent.CopyOnWriteArraySet",
                false).getVisitor();
    }
}
