package io.smallrye.reactive.messaging.kafka.companion;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collection;

import org.apache.kafka.common.Node;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.kafka.companion.test.KafkaCompanionTestBase;

public class ClusterTest extends KafkaCompanionTestBase {

    @Test
    void testClusterInfo() {
        ClusterCompanion clusterCompanion = companion.cluster();
        Collection<Node> nodes = clusterCompanion.nodes();
        assertThat(nodes).hasSize(1);
        assertThat(clusterCompanion.controller()).isIn(nodes);
        assertThat(clusterCompanion.clusterId()).isNotNull();
        assertThat(clusterCompanion.aclOperations()).isNotEmpty();
    }

}
