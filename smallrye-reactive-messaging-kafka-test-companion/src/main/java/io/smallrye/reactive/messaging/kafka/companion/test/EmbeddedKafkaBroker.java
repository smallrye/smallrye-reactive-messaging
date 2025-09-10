package io.smallrye.reactive.messaging.kafka.companion.test;

import static org.apache.kafka.common.security.auth.SecurityProtocol.PLAINTEXT;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.coordinator.transaction.TransactionLogConfig;
import org.apache.kafka.metadata.storage.Formatter;
import org.apache.kafka.network.SocketServerConfigs;
import org.apache.kafka.raft.QuorumConfig;
import org.apache.kafka.server.config.KRaftConfigs;
import org.apache.kafka.server.config.ReplicationConfigs;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.storage.internals.log.CleanerConfig;
import org.jboss.logging.Logger;

import kafka.server.KafkaConfig;
import kafka.server.KafkaRaftServer;
import scala.jdk.javaapi.StreamConverters;

/**
 * Embedded KRaft Broker, by default listens on localhost with random broker and controller ports.
 * <p>
 */
public class EmbeddedKafkaBroker implements Closeable {

    private static final Logger LOGGER = Logger.getLogger(EmbeddedKafkaBroker.class.getName());

    private static final String COMPANION_BROKER_PREFIX = "companion-embedded-kafka";

    private KafkaRaftServer kafkaServer;
    private KafkaConfig config;

    private int nodeId = 1;
    private String host = "localhost";
    private int kafkaPort = 0;
    private int controllerPort = 0;
    private boolean deleteDirsOnClose = true;
    private String clusterId = Uuid.randomUuid().toString();
    private final List<Endpoint> advertisedListeners = new ArrayList<>();
    private Consumer<Properties> brokerConfigModifier;

    /**
     * Configure node id for the broker.
     *
     * @param nodeId the node id.
     * @return this {@link EmbeddedKafkaBroker}
     */
    public EmbeddedKafkaBroker withNodeId(int nodeId) {
        assertNotRunning();
        this.nodeId = nodeId;
        return this;
    }

    /**
     * Configure properties for the broker.
     *
     * @param function the config modifier function.
     * @return this {@link EmbeddedKafkaBroker}
     */
    public EmbeddedKafkaBroker withAdditionalProperties(Consumer<Properties> function) {
        assertNotRunning();
        this.brokerConfigModifier = function;
        return this;
    }

    /**
     * Configure the port on which the broker will listen.
     *
     * @param port the port.
     * @return this {@link EmbeddedKafkaBroker}
     */
    public EmbeddedKafkaBroker withKafkaPort(int port) {
        assertNotRunning();
        this.kafkaPort = port;
        return this;
    }

    /**
     * Configure the controller port for the broker.
     *
     * @param port the port.
     * @return this {@link EmbeddedKafkaBroker}
     */
    public EmbeddedKafkaBroker withControllerPort(int port) {
        assertNotRunning();
        this.controllerPort = port;
        return this;
    }

    /**
     * Configure the hostname on which the broker will listen.
     *
     * @param host the host.
     * @return this {@link EmbeddedKafkaBroker}
     */
    public EmbeddedKafkaBroker withKafkaHost(String host) {
        assertNotRunning();
        this.host = host;
        return this;
    }

    /**
     * Configure the cluster id for the broker storage dirs.
     *
     * @param clusterId the cluster id.
     * @return this {@link EmbeddedKafkaBroker}
     */
    public EmbeddedKafkaBroker withClusterId(String clusterId) {
        assertNotRunning();
        this.clusterId = clusterId;
        return this;
    }

    /**
     * Configure whether log directories will be deleted on broker shutdown.
     *
     * @param deleteDirsOnClose {@code true}
     * @return this {@link EmbeddedKafkaBroker}
     */
    public EmbeddedKafkaBroker withDeleteLogDirsOnClose(boolean deleteDirsOnClose) {
        assertNotRunning();
        this.deleteDirsOnClose = deleteDirsOnClose;
        return this;
    }

    /**
     * Configure custom listeners for the broker.
     * <p>
     * Note that this will override the default PLAINTEXT listener.
     * A CONTROLLER listener will be added automatically.
     *
     * @return this {@link EmbeddedKafkaBroker}
     */
    public EmbeddedKafkaBroker withAdvertisedListeners(Endpoint... endpoints) {
        assertNotRunning();
        this.advertisedListeners.addAll(Arrays.asList(endpoints));
        return this;
    }

    /**
     * Configure custom listeners for the broker.
     * <p>
     * Note that this will override the default PLAINTEXT listener.
     * A CONTROLLER listener will be added automatically.
     *
     * @return this {@link EmbeddedKafkaBroker}
     */
    public EmbeddedKafkaBroker withAdvertisedListeners(String advertisedListeners) {
        assertNotRunning();
        String[] listeners = advertisedListeners.split(",");
        for (String listener : listeners) {
            this.advertisedListeners.add(parseEndpoint(listener));
        }
        return this;
    }

    /**
     * Create and start the broker.
     *
     * @return this {@link EmbeddedKafkaBroker}
     */
    public synchronized EmbeddedKafkaBroker start() {
        if (isRunning()) {
            return this;
        }

        Endpoint internalEndpoint = EmbeddedKafkaBroker.endpoint(PLAINTEXT, host, kafkaPort);
        Endpoint controller = EmbeddedKafkaBroker.controller(host, controllerPort);
        Properties properties = createDefaultBrokerConfig(nodeId, controller, internalEndpoint, advertisedListeners);

        if (brokerConfigModifier != null) {
            brokerConfigModifier.accept(properties);
        }

        if (properties.get(ServerLogConfigs.LOG_DIR_CONFIG) == null) {
            createAndSetlogDir(properties);
        }

        long start = System.currentTimeMillis();
        this.config = formatStorageFromConfig(properties, clusterId, true);
        this.kafkaServer = createServer(config);
        LOGGER.infof("Kafka broker started in %d ms with advertised listeners: %s",
                System.currentTimeMillis() - start, getAdvertisedListeners());
        return this;
    }

    @Override
    public synchronized void close() {
        try {
            if (isRunning()) {
                kafkaServer.shutdown();
                kafkaServer.awaitShutdown();
            }
        } catch (Exception e) {
            LOGGER.error("Error shutting down broker", e);
        } finally {
            if (deleteDirsOnClose) {
                try {
                    for (String logDir : getLogDirs()) {
                        Utils.delete(new File(logDir));
                    }
                } catch (Exception e) {
                    LOGGER.error("Error deleting logdirs", e);
                }
            }
            kafkaServer = null;
        }
    }

    public boolean isRunning() {
        return kafkaServer != null;
    }

    private void assertNotRunning() {
        if (isRunning()) {
            throw new IllegalStateException("Configuration of the running broker is not permitted.");
        }
    }

    public KafkaConfig getKafkaConfig() {
        return config;
    }

    public String getAdvertisedListeners() {
        return getAdvertisedListeners(config);
    }

    public List<String> getLogDirs() {
        return config.logDirs();
    }

    public int getNodeId() {
        return this.nodeId;
    }

    public String getClusterId() {
        return this.clusterId;
    }

    public static Endpoint endpoint(SecurityProtocol protocol, int port) {
        return EmbeddedKafkaBroker.endpoint(protocol.name, protocol, "", port);
    }

    public static Endpoint endpoint(SecurityProtocol protocol, String host, int port) {
        return EmbeddedKafkaBroker.endpoint(protocol.name, protocol, host, port);
    }

    public static Endpoint endpoint(String listener, SecurityProtocol protocol, int port) {
        return EmbeddedKafkaBroker.endpoint(listener, protocol, "", port);
    }

    public static Endpoint endpoint(String listener, SecurityProtocol protocol, String host, int port) {
        return new Endpoint(listener, protocol, host, getUnusedPort(port));
    }

    public static Endpoint parseEndpoint(SecurityProtocol protocol, String listenerStr) {
        Endpoint endpoint = parseEndpoint(listenerStr);
        return new Endpoint(listenerName(endpoint), protocol, endpoint.host(), endpoint.port());
    }

    public static Endpoint parseEndpoint(String listenerStr) {
        String[] parts = listenerStr.split(":");
        if (parts.length == 2) {
            return new Endpoint(null, PLAINTEXT, parts[0], Integer.parseInt(parts[1]));
        } else if (parts.length == 3) {
            String listenerName = parts[0];
            String host = parts[1].replace("//", "");
            int port = Integer.parseInt(parts[2]);
            return new Endpoint(listenerName, SecurityProtocol.forName(listenerName), host, port);
        }
        throw new IllegalArgumentException("Cannot parse listener: " + listenerStr);
    }

    public static Properties createDefaultBrokerConfig(int nodeId, Endpoint controller, Endpoint internalEndpoint,
            List<Endpoint> advertisedListeners) {
        Properties props = new Properties();
        props.put(ServerConfigs.BROKER_ID_CONFIG, Integer.toString(nodeId));

        // Configure kraft
        props.put(KRaftConfigs.PROCESS_ROLES_CONFIG, "broker,controller");
        props.put(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, listenerName(controller));
        props.put(QuorumConfig.QUORUM_VOTERS_CONFIG, nodeId + "@" + controller.host() + ":" + controller.port());

        // Configure listeners
        Map<String, Endpoint> listeners = advertisedListeners.stream()
                .map(l -> new Endpoint(l.listener(), l.securityProtocol(), "", l.port()))
                .collect(Collectors.toMap(EmbeddedKafkaBroker::listenerName, Function.identity()));
        listeners.put(listenerName(controller), controller);
        listeners.put(listenerName(internalEndpoint), internalEndpoint);

        String listenersString = listeners.values().stream()
                .map(EmbeddedKafkaBroker::toListenerString)
                .distinct()
                .collect(Collectors.joining(","));
        props.put(SocketServerConfigs.LISTENERS_CONFIG, listenersString);

        // Find a PLAINTEXT listener
        Endpoint plaintextEndpoint = advertisedListeners.stream()
                .filter(e -> e.securityProtocol() == PLAINTEXT)
                .findFirst().orElse(internalEndpoint);

        // Configure advertised listeners
        String advertisedListenersString = advertisedListeners.stream()
                .map(EmbeddedKafkaBroker::toListenerString)
                .distinct()
                .collect(Collectors.joining(","));
        if (!Utils.isBlank(advertisedListenersString)) {
            props.put(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG, advertisedListenersString);
        }

        // Configure security protocol map
        String securityProtocolMap = listeners.values().stream()
                .map(EmbeddedKafkaBroker::toProtocolMap)
                .distinct()
                .collect(Collectors.joining(","));
        props.put(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG, securityProtocolMap);
        props.put(ReplicationConfigs.INTER_BROKER_LISTENER_NAME_CONFIG, listenerName(plaintextEndpoint));

        // Configure static default props
        props.put(ReplicationConfigs.REPLICA_SOCKET_TIMEOUT_MS_CONFIG, "1000");
        props.put(ReplicationConfigs.REPLICA_HIGH_WATERMARK_CHECKPOINT_INTERVAL_MS_CONFIG, String.valueOf(Long.MAX_VALUE));
        props.put(ReplicationConfigs.CONTROLLER_SOCKET_TIMEOUT_MS_CONFIG, "1000");
        props.put(ServerConfigs.CONTROLLED_SHUTDOWN_ENABLE_CONFIG, Boolean.toString(false));
        props.put(ServerConfigs.DELETE_TOPIC_ENABLE_CONFIG, Boolean.toString(true));
        props.put(ServerLogConfigs.LOG_DELETE_DELAY_MS_CONFIG, "1000");
        props.put(CleanerConfig.LOG_CLEANER_DEDUPE_BUFFER_SIZE_PROP, "2097152");
        props.put(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, "1");
        props.put(GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, "5");
        props.put(GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, "0");
        props.putIfAbsent(TopicConfig.MESSAGE_TIMESTAMP_AFTER_MAX_MS_CONFIG, String.valueOf(Long.MAX_VALUE));
        props.putIfAbsent(TopicConfig.MESSAGE_TIMESTAMP_BEFORE_MAX_MS_CONFIG, String.valueOf(Long.MAX_VALUE));
        props.put(ServerLogConfigs.NUM_PARTITIONS_CONFIG, "1");
        props.put(ReplicationConfigs.DEFAULT_REPLICATION_FACTOR_CONFIG, "1");
        props.putIfAbsent(TransactionLogConfig.TRANSACTIONS_TOPIC_REPLICATION_FACTOR_CONFIG, "1");
        props.putIfAbsent(TransactionLogConfig.TRANSACTIONS_TOPIC_MIN_ISR_CONFIG, "1");

        return props;
    }

    public static KafkaConfig formatStorageFromConfig(Properties properties, String clusterId, boolean ignoreFormatted) {
        KafkaConfig config = KafkaConfig.fromProps(properties, false);
        Formatter formatter = new Formatter();
        formatter.setClusterId(clusterId)
                .setNodeId(config.nodeId())
                .setUnstableFeatureVersionsEnabled(config.unstableFeatureVersionsEnabled())
                .setIgnoreFormatted(ignoreFormatted)
                .setControllerListenerName(config.controllerListenerNames().get(0))
                .setMetadataLogDirectory(config.metadataLogDir());
        configToLogDirectories(config).forEach(formatter::addDirectory);
        try {
            formatter.run();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return config;
    }

    static Set<String> configToLogDirectories(KafkaConfig config) {
        TreeSet<String> dirs = new TreeSet<>(config.logDirs());
        String metadataLogDir = config.metadataLogDir();
        if (metadataLogDir != null) {
            dirs.add(metadataLogDir);
        }
        return dirs;
    }

    public static void formatStorage(List<String> directories, String controllerListenerName,
            String metadataLogDirectory,
            String clusterId, int nodeId, boolean ignoreFormatted) {
        Formatter formatter = new Formatter();
        formatter.setClusterId(clusterId)
                .setNodeId(nodeId)
                .setIgnoreFormatted(ignoreFormatted)
                .setControllerListenerName(controllerListenerName)
                .setMetadataLogDirectory(metadataLogDirectory);
        directories.forEach(formatter::addDirectory);
        try {
            formatter.run();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void formatStorage(List<String> directories, String clusterId, int nodeId, boolean ignoreFormatted) {
        formatStorage(directories, "CONTROLLER", directories.get(0), clusterId, nodeId, ignoreFormatted);
    }

    public static KafkaRaftServer createServer(final KafkaConfig config) {
        KafkaRaftServer server = new KafkaRaftServer(config, Time.SYSTEM);
        server.startup();
        return server;
    }

    private static String getAdvertisedListeners(KafkaConfig config) {
        return StreamConverters.asJavaParStream(config.effectiveAdvertisedBrokerListeners())
                .map(EmbeddedKafkaBroker::toListenerString)
                .collect(Collectors.joining(","));
    }

    private static int getUnusedPort(int port) {
        if (port != 0) {
            return port;
        }
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void createAndSetlogDir(Properties properties) {
        try {
            properties.put(ServerLogConfigs.LOG_DIR_CONFIG,
                    Files.createTempDirectory(COMPANION_BROKER_PREFIX + "-" + UUID.randomUUID()).toString());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static Endpoint controller(String host, int port) {
        return EmbeddedKafkaBroker.endpoint("CONTROLLER", PLAINTEXT, host, port);
    }

    public static String toListenerString(Endpoint endpoint) {
        return String.format("%s://%s:%d", listenerName(endpoint), endpoint.host(), endpoint.port());
    }

    private static String toProtocolMap(Endpoint endpoint) {
        return String.format("%s:%s", listenerName(endpoint), endpoint.securityProtocol().name);
    }

    private static String listenerName(Endpoint endpoint) {
        String listener = endpoint.listener();
        if (listener != null && !listener.isBlank()) {
            return listener;
        }
        return endpoint.securityProtocol().name;
    }

    public static class LoggingOutputStream extends java.io.OutputStream {

        public static PrintStream loggerPrintStream(Logger logger) {
            return new PrintStream(new LoggingOutputStream(logger));
        }

        private final ByteArrayOutputStream os = new ByteArrayOutputStream(1000);
        private final Logger logger;

        LoggingOutputStream(Logger logger) {
            this.logger = logger;
        }

        @Override
        public void write(int b) throws IOException {
            if (b == '\n' || b == '\r') {
                os.flush();
                String log = os.toString();
                logger.info(log);
            } else {
                os.write(b);
            }
        }
    }

}
