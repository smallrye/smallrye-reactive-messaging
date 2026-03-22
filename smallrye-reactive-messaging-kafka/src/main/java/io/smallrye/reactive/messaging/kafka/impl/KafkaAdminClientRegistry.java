package io.smallrye.reactive.messaging.kafka.impl;

import static io.smallrye.reactive.messaging.kafka.i18n.KafkaLogging.log;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import io.smallrye.reactive.messaging.kafka.KafkaAdmin;

@ApplicationScoped
public class KafkaAdminClientRegistry {

    private final ConcurrentHashMap<String, SharedAdmin> adminClients = new ConcurrentHashMap<>();
    private final AtomicInteger adminClientCounter = new AtomicInteger();

    @Inject
    @ConfigProperty(name = "smallrye.messaging.kafka.admin-client.pooling.enabled", defaultValue = "false")
    boolean poolingEnabled;

    static class SharedAdmin {
        final KafkaAdmin admin;
        int refCount;

        SharedAdmin(KafkaAdmin admin) {
            this.admin = admin;
            this.refCount = 1;
        }

        boolean release() {
            return --refCount <= 0;
        }
    }

    public KafkaAdmin getOrCreateAdminClient(Map<String, Object> config, String channel, boolean incoming) {
        Map<String, Object> normalized = normalizeAdminConfig(config);
        if (poolingEnabled) {
            SharedAdmin entry = adminClients.compute(computeKey(normalized), (k, existing) -> {
                if (existing != null) {
                    existing.refCount++;
                    return existing;
                }
                return new SharedAdmin(
                        createAdminClient(normalized, "smallrye-kafka-admin-" + adminClientCounter.incrementAndGet()));
            });
            return entry.admin;
        } else {
            String clientId = adminClientName(config, channel, incoming);
            return createAdminClient(normalized, clientId);
        }
    }

    static String adminClientName(Map<String, Object> config, String channel, boolean incoming) {
        Object id = config.get(ConsumerConfig.CLIENT_ID_CONFIG);
        if (id != null) {
            return "kafka-admin-" + id + "-" + channel;
        } else {
            return "kafka-admin-" + (incoming ? "incoming-" : "outgoing-") + channel;
        }
    }

    /**
     * Extracts admin-client-relevant properties from the given config map.
     * Filters to {@link AdminClientConfig} keys and {@code sasl.*} properties,
     * excludes {@code client.id}, and applies the default reconnect backoff if not set.
     */
    static Map<String, Object> normalizeAdminConfig(Map<String, Object> config) {
        TreeMap<String, Object> normalized = new TreeMap<>();
        for (Map.Entry<String, Object> entry : config.entrySet()) {
            if (AdminClientConfig.configNames().contains(entry.getKey()) || entry.getKey().startsWith("sasl.")) {
                normalized.put(entry.getKey(), entry.getValue().toString());
            }
        }
        // Exclude client.id as it varies per channel and is assigned by the registry
        normalized.remove(AdminClientConfig.CLIENT_ID_CONFIG);
        // If no backoff is set, use 10s, it avoids high load on disconnection.
        if (!normalized.containsKey(AdminClientConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG)) {
            normalized.put(AdminClientConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, "10000");
        }
        return normalized;
    }

    private static KafkaAdmin createAdminClient(Map<String, Object> normalizedConfig, String clientId) {
        Map<String, Object> copy = new HashMap<>(normalizedConfig);
        copy.put(AdminClientConfig.CLIENT_ID_CONFIG, clientId);
        return new ReactiveKafkaAdminClient(copy);
    }

    static String computeKey(Map<String, Object> normalizedConfig) {
        return sha256(normalizedConfig.toString());
    }

    static String sha256(String value) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(64);
            for (byte b : digest) {
                sb.append(Integer.toHexString((b & 0xFF) | 0x100), 1, 3);
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    public void release(KafkaAdmin admin) {
        if (admin == null) {
            return;
        }
        if (poolingEnabled) {
            for (var e : adminClients.entrySet()) {
                if (e.getValue().admin == admin) {
                    if (adminClients.computeIfPresent(e.getKey(),
                            (k, entry) -> entry.release() ? null : entry) == null) {
                        closeQuietly(admin);
                    }
                    return;
                }
            }
        } else {
            closeQuietly(admin);
        }
    }

    public void close() {
        adminClients.forEach((key, entry) -> closeQuietly(entry.admin));
        adminClients.clear();
    }

    private static void closeQuietly(KafkaAdmin admin) {
        try {
            admin.closeAndAwait();
        } catch (Throwable e) {
            log.exceptionOnClose(e);
        }
    }
}
