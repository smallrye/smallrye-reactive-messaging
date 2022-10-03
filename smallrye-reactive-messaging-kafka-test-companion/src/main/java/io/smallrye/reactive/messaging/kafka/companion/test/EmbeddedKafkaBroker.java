package io.smallrye.reactive.messaging.kafka.companion.test;

import static io.smallrye.reactive.messaging.kafka.companion.test.EmbeddedKafkaBroker.LoggingOutputStream.loggerPrintStream;
import static org.apache.kafka.common.security.auth.SecurityProtocol.PLAINTEXT;
import static org.apache.kafka.server.common.MetadataVersion.MINIMUM_BOOTSTRAP_VERSION;

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
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.jboss.logging.Logger;

import kafka.cluster.EndPoint;
import kafka.server.KafkaConfig;
import kafka.server.KafkaRaftServer;
import kafka.server.MetaProperties;
import kafka.tools.StorageTool;
import scala.Option;
import scala.collection.immutable.Seq;
import scala.jdk.CollectionConverters;
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

        if (properties.get(KafkaConfig.LogDirProp()) == null) {
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
        return getLogDirs(config);
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
        return new Endpoint(endpoint.listenerName().orElse(protocol.name), protocol, endpoint.host(), endpoint.port());
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
        props.put(KafkaConfig.BrokerIdProp(), Integer.toString(nodeId));

        // Configure kraft
        props.put(KafkaConfig.ProcessRolesProp(), "broker,controller");
        props.put(KafkaConfig.ControllerListenerNamesProp(), listenerName(controller));
        props.put(KafkaConfig.QuorumVotersProp(), nodeId + "@" + controller.host() + ":" + controller.port());

        // Configure listeners
        Map<String, Endpoint> listeners = advertisedListeners.stream()
                .map(l -> new Endpoint(l.listenerName().orElse(null), l.securityProtocol(), "", l.port()))
                .collect(Collectors.toMap(EmbeddedKafkaBroker::listenerName, Function.identity()));
        listeners.put(listenerName(controller), controller);
        listeners.put(listenerName(internalEndpoint), internalEndpoint);

        String listenersString = listeners.values().stream()
                .map(EmbeddedKafkaBroker::toListenerString)
                .distinct()
                .collect(Collectors.joining(","));
        props.put(KafkaConfig.ListenersProp(), listenersString);

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
            props.put(KafkaConfig.AdvertisedListenersProp(), advertisedListenersString);
        }

        // Configure security protocol map
        String securityProtocolMap = listeners.values().stream()
                .map(EmbeddedKafkaBroker::toProtocolMap)
                .distinct()
                .collect(Collectors.joining(","));
        props.put(KafkaConfig.ListenerSecurityProtocolMapProp(), securityProtocolMap);
        props.put(KafkaConfig.InterBrokerListenerNameProp(), listenerName(plaintextEndpoint));

        // Configure static default props
        props.put(KafkaConfig.ReplicaSocketTimeoutMsProp(), "1000");
        props.put(KafkaConfig.ReplicaHighWatermarkCheckpointIntervalMsProp(), String.valueOf(Long.MAX_VALUE));
        props.put(KafkaConfig.ControllerSocketTimeoutMsProp(), "1000");
        props.put(KafkaConfig.ControlledShutdownEnableProp(), Boolean.toString(false));
        props.put(KafkaConfig.ControlledShutdownRetryBackoffMsProp(), "100");
        props.put(KafkaConfig.DeleteTopicEnableProp(), Boolean.toString(true));
        props.put(KafkaConfig.LogDeleteDelayMsProp(), "1000");
        props.put(KafkaConfig.LogCleanerDedupeBufferSizeProp(), "2097152");
        props.put(KafkaConfig.LogMessageTimestampDifferenceMaxMsProp(), String.valueOf(Long.MAX_VALUE));
        props.put(KafkaConfig.OffsetsTopicReplicationFactorProp(), "1");
        props.put(KafkaConfig.OffsetsTopicPartitionsProp(), "5");
        props.put(KafkaConfig.GroupInitialRebalanceDelayMsProp(), "0");
        props.put(KafkaConfig.NumPartitionsProp(), "1");
        props.put(KafkaConfig.DefaultReplicationFactorProp(), "1");

        return props;
    }

    public static KafkaConfig formatStorageFromConfig(Properties properties, String clusterId, boolean ignoreFormatted) {
        KafkaConfig config = KafkaConfig.fromProps(properties, false);
        Seq<String> directories = StorageTool.configToLogDirectories(config);
        MetaProperties metaProperties = StorageTool.buildMetadataProperties(clusterId, config);
        StorageTool.formatCommand(loggerPrintStream(LOGGER), directories, metaProperties, MINIMUM_BOOTSTRAP_VERSION,
                ignoreFormatted);
        return config;
    }

    public static void formatStorage(List<String> directories, String clusterId, int nodeId, boolean ignoreFormatted) {
        MetaProperties metaProperties = new MetaProperties(clusterId, nodeId);
        Seq<String> dirs = CollectionConverters.ListHasAsScala(directories).asScala().toSeq();
        StorageTool.formatCommand(loggerPrintStream(LOGGER), dirs, metaProperties, MINIMUM_BOOTSTRAP_VERSION, ignoreFormatted);
    }

    public static KafkaRaftServer createServer(final KafkaConfig config) {
        KafkaRaftServer server = new KafkaRaftServer(config, Time.SYSTEM, Option.apply(COMPANION_BROKER_PREFIX));
        server.startup();
        return server;
    }

    private static String getAdvertisedListeners(KafkaConfig config) {
        return StreamConverters.asJavaParStream(config.effectiveAdvertisedListeners())
                .map(EndPoint::connectionString)
                .collect(Collectors.joining(","));
    }

    private static List<String> getLogDirs(KafkaConfig config) {
        return StreamConverters.asJavaParStream(config.logDirs())
                .collect(Collectors.toList());
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
            properties.put(KafkaConfig.LogDirProp(),
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
        return endpoint.listenerName().orElse(endpoint.securityProtocol().name);
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
