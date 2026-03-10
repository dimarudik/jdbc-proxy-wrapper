package dev.humus;

import dev.humus.discovery.DatabaseDiscoveryServiceGrpc;
import dev.humus.discovery.DiscoveryRequest;
import dev.humus.discovery.DiscoveryResponse;
import dev.humus.discovery.InstanceType;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
public class HumusDriverTest {
    private static final Logger log = LoggerFactory.getLogger(HumusDriverTest.class);

    @Container
    static PostgreSQLContainer<?> masterDb = new PostgreSQLContainer<>("postgres")
            .withDatabaseName("humus_db").withUsername("user").withPassword("pass");

    @Container
    static PostgreSQLContainer<?> replicaDb = new PostgreSQLContainer<>("postgres")
            .withDatabaseName("humus_db").withUsername("user").withPassword("pass");

    private static Server grpcServer;
    private static final int GRPC_PORT = 9091;

    // Состояние для мока gRPC: какой тип инстанса возвращать
    private static final AtomicReference<InstanceType> nextType = new AtomicReference<>(InstanceType.MASTER);

    @BeforeAll
    static void setup() throws Exception {
        grpcServer = ServerBuilder.forPort(GRPC_PORT)
                .addService(new DatabaseDiscoveryServiceGrpc.DatabaseDiscoveryServiceImplBase() {
                    @Override
                    public void getDatabaseInstance(DiscoveryRequest req, StreamObserver<DiscoveryResponse> obs) {
                        InstanceType type = nextType.get();
                        var db = (type == InstanceType.MASTER) ? masterDb : replicaDb;

                        DiscoveryResponse resp = DiscoveryResponse.newBuilder()
                                .setHost(db.getHost())
                                .setPort(db.getMappedPort(5432))
                                .setInstanceType(type)
                                .build();
                        obs.onNext(resp);
                        obs.onCompleted();
                    }
                }).build().start();
    }

    @AfterAll
    static void tearDown() { if (grpcServer != null) grpcServer.shutdownNow(); }

    @Test
    @DisplayName("Тест переключения Master -> Replica через setReadOnly")
    void testReadWriteSplitting() throws Exception {
        Class.forName("dev.humus.HumusDriver");
        String url = "jdbc:humus:grpc://localhost:" + GRPC_PORT + "/humus_db";

        Properties props = new Properties();
        props.setProperty("user", "user");
        props.setProperty("password", "pass");

        try (Connection conn = DriverManager.getConnection(url, props)) {
            // 1. По умолчанию мы на Master
            assertFalse(conn.isReadOnly());
            int masterPort = getInternalPort(conn);
            assertEquals(masterDb.getMappedPort(5432), masterPort);

            // 2. Переключаемся на Replica
            nextType.set(InstanceType.ASYNC_REPLICA);
            conn.setReadOnly(true); // Тут срабатывает Hot Swap в плагине

            assertTrue(conn.isReadOnly());
            int replicaPort = getInternalPort(conn);
            assertEquals(replicaDb.getMappedPort(5432), replicaPort);
            assertNotEquals(masterPort, replicaPort);

            log.info("Hot Swap success! Switched from port {} to {}", masterPort, replicaPort);
        }
    }

    @Test
    @DisplayName("Запрет переключения внутри транзакции")
    void testNoSwitchInTransaction() throws Exception {
        String url = "jdbc:humus:grpc://localhost:" + GRPC_PORT + "/humus_db";
        try (Connection conn = DriverManager.getConnection(url, "user", "pass")) {
            conn.setAutoCommit(false); // Начинаем транзакцию

            assertThrows(java.sql.SQLException.class, () -> {
                conn.setReadOnly(true);
            }, "Должно быть исключение при попытке свитча в транзакции");
        }
    }

    private int getInternalPort(Connection conn) throws Exception {
        // Вытягиваем реальный порт через метаданные, чтобы убедиться в смене сокета
        String url = conn.getMetaData().getURL();
        // url формат: jdbc:postgresql://localhost:PORT/db
        return Integer.parseInt(url.split(":")[3].split("/")[0]);
    }
}
