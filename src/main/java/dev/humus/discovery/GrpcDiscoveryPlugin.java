package dev.humus.discovery;

import dev.humus.core.ConnectionWrapper;
import dev.humus.core.JdbcCallable;
import dev.humus.core.ProxyPlugin;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

public class GrpcDiscoveryPlugin implements ProxyPlugin {
    private static final Logger log = LoggerFactory.getLogger(GrpcDiscoveryPlugin.class);

    private final String discoveryServiceAddr;
    private final String dbClusterName;

    public GrpcDiscoveryPlugin(String discoveryServiceAddr, String dbClusterName) {
        this.discoveryServiceAddr = discoveryServiceAddr;
        this.dbClusterName = dbClusterName;
    }

    @Override
    public <W, T, R> R execute(W wrapper, T target, String methodName, JdbcCallable<T, R> next, Object[] args)
            throws SQLException {

        if ("setReadOnly".equals(methodName)) {
            boolean requestedReadOnly = (boolean) args[0];
            ConnectionWrapper connWrapper = (ConnectionWrapper) wrapper;

            // Проверка: можно ли сейчас переключаться?
            if (!connWrapper.isSafeToSwitch()) {
                throw new SQLException("Cannot switch to " +
                        (requestedReadOnly ? "Replica" : "Master") +
                        " inside an active transaction. Commit or rollback first.");
            }

            // Если хост уже соответствует типу (нужна проверка текущего типа в плагине), выходим
            // ... (логика Discovery из прошлых шагов)

            // Hot Swap
            DiscoveryResponse node = resolve();
            String newUrl = String.format("jdbc:postgresql://%s:%d/%s",
                    node.getHost(), node.getPort(), dbClusterName);

            Connection newConn = DriverManager.getConnection(newUrl, connWrapper.getConnectInfo());
            newConn.setReadOnly(requestedReadOnly);
            newConn.setAutoCommit(true); // Свитч возможен только в этом режиме

            connWrapper.updateTarget(newConn);
            log.info("Successfully switched to {} node", node.getInstanceType());

            return null;
        }

        return next.call(target, args);
    }

    private DiscoveryResponse callDiscoveryService(String serviceName) throws SQLException {
        ManagedChannel channel = ManagedChannelBuilder.forTarget(discoveryServiceAddr)
                .usePlaintext() // Для разработки, в продакшене обычно TLS
                .build();

        try {
            // Создаем блокирующий стаб (сгенерирован из .proto)
            DatabaseDiscoveryServiceGrpc.DatabaseDiscoveryServiceBlockingStub stub =
                    DatabaseDiscoveryServiceGrpc.newBlockingStub(channel);

            DiscoveryRequest request = DiscoveryRequest.newBuilder()
                    .setServiceName(serviceName)
                    .build();

            // Вызываем удаленный метод с таймаутом
            return stub.withDeadlineAfter(5, TimeUnit.SECONDS)
                    .getDatabaseInstance(request);

        } catch (Exception e) {
            log.error("Failed to discover database via gRPC: {}", e.getMessage());
            throw new SQLException("Discovery service unavailable", e);
        } finally {
            channel.shutdown();
            try {
                if (!channel.awaitTermination(1, TimeUnit.SECONDS)) {
                    channel.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public DiscoveryResponse resolve() throws SQLException {
        return callDiscoveryService(this.dbClusterName);
    }
}
