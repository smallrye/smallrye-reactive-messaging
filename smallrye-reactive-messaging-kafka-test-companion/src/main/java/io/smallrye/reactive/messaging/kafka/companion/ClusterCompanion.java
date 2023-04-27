package io.smallrye.reactive.messaging.kafka.companion;

import static io.smallrye.reactive.messaging.kafka.companion.KafkaCompanion.toUni;

import java.time.Duration;
import java.util.Collection;
import java.util.Set;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.acl.AclOperation;

/**
 * Companion for Cluster operations on Kafka broker
 */
public class ClusterCompanion {
    private final AdminClient adminClient;
    private final Duration kafkaApiTimeout;

    public ClusterCompanion(AdminClient adminClient, Duration kafkaApiTimeout) {
        this.adminClient = adminClient;
        this.kafkaApiTimeout = kafkaApiTimeout;
    }

    /**
     * @return the collection of {@link Node}s of the cluster
     */
    public Collection<Node> nodes() {
        return toUni(() -> adminClient.describeCluster().nodes()).await().atMost(kafkaApiTimeout);
    }

    /**
     * @return the controller {@link Node} of the cluster
     */
    public Node controller() {
        return toUni(() -> adminClient.describeCluster().controller()).await().atMost(kafkaApiTimeout);
    }

    /**
     * @return the cluster id
     */
    public String clusterId() {
        return toUni(() -> adminClient.describeCluster().clusterId()).await().atMost(kafkaApiTimeout);
    }

    /**
     * @return the set of {@link AclOperation}s
     */
    public Set<AclOperation> aclOperations() {
        return toUni(() -> adminClient.describeCluster(new DescribeClusterOptions().includeAuthorizedOperations(true))
                .authorizedOperations()).await().atMost(kafkaApiTimeout);
    }

}
