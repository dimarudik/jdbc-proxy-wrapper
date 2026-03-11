package dev.humus.discovery;

import dev.humus.core.JdbcCallable;
import dev.humus.core.ProxyPlugin;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class GrpcDiscoveryPlugin implements ProxyPlugin {
    private static final Logger logger = Logger.getLogger(GrpcDiscoveryPlugin.class.getName());

    private static final Map<String, CachedResponse> CACHE = new ConcurrentHashMap<>();
    private static final long CACHE_TTL_MS = TimeUnit.MINUTES.toMillis(5);

    private final String discoveryAddr;
    private final String dbClusterName;

    public GrpcDiscoveryPlugin(String discoveryAddr, String dbClusterName) {
        this.discoveryAddr = discoveryAddr;
        this.dbClusterName = dbClusterName;
    }

    public DiscoveryResponse resolve() throws SQLException {
        CachedResponse cached = CACHE.get(dbClusterName);

        if (cached != null && !cached.isExpired()) {
            logger.log(Level.FINE, "Using cached discovery for cluster: {0}", dbClusterName);
            return cached.response;
        }

        logger.log(Level.INFO, "Cache miss or expired. Fetching discovery for: {0}", dbClusterName);
        try {
            DiscoveryResponse freshResponse = fetchFromGrpc();
            CACHE.put(dbClusterName, new CachedResponse(freshResponse));
            return freshResponse;
        } catch (SQLException e) {
            // Если gRPC упал, но в кеше есть старые данные — используем их как fallback
            if (cached != null) {
                logger.log(Level.WARNING, "Discovery failed, using stale cache for {0}", dbClusterName);
                return cached.response;
            }
            throw e;
        }
    }

    /**
     * Позволяет принудительно сбросить кеш (например, при ошибке соединения)
     */
    public static void invalidateCache(String clusterName) {
        CACHE.remove(clusterName);
    }

    private DiscoveryResponse fetchFromGrpc() throws SQLException {
        ManagedChannel channel = ManagedChannelBuilder.forTarget(discoveryAddr)
                .usePlaintext()
                .build();
        try {
            var stub = DatabaseDiscoveryServiceGrpc.newBlockingStub(channel);
            return stub.withDeadlineAfter(5, TimeUnit.SECONDS)
                    .getDatabaseInstance(DiscoveryRequest.newBuilder()
                            .setServiceName(dbClusterName)
                            .build());
        } catch (Exception e) {
            logger.log(Level.SEVERE, "gRPC Discovery failed: {0}", e.getMessage());
            throw new SQLException("Discovery service unavailable", e);
        } finally {
            channel.shutdown();
        }
    }

    @Override
    public <W, T, R> R execute(W wrapper, T target, String methodName, JdbcCallable<T, R> next, Object[] args)
            throws SQLException {
        try {
            return next.call(target, args);
        } catch (SQLException e) {
            // Если получили ошибку связи (SQLState 08***), инвалидируем кеш
            if (e.getSQLState() != null && e.getSQLState().startsWith("08")) {
                logger.log(Level.WARNING, "Connection error detected, invalidating discovery cache for {0}", dbClusterName);
                invalidateCache(dbClusterName);
            }
            throw e;
        }
    }

    private static class CachedResponse {
        final DiscoveryResponse response;
        final long expirationTime;

        CachedResponse(DiscoveryResponse response) {
            this.response = response;
            this.expirationTime = System.currentTimeMillis() + CACHE_TTL_MS;
        }

        boolean isExpired() {
            return System.currentTimeMillis() > expirationTime;
        }
    }
}
