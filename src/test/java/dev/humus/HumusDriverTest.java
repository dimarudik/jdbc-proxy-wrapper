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
import org.slf4j.bridge.SLF4JBridgeHandler;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
public class HumusDriverTest {
    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres")
            .withDatabaseName("humus_db")
            .withUsername("user")
            .withPassword("pass");

    private static Server grpcServer;
    private static final AtomicInteger discoveryCalls = new AtomicInteger(0);
    private static final int GRPC_PORT = 9091;

    @BeforeAll
    static void setup() throws Exception {
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();

        grpcServer = ServerBuilder.forPort(GRPC_PORT)
                .addService(new DatabaseDiscoveryServiceGrpc.DatabaseDiscoveryServiceImplBase() {
                    @Override
                    public void getDatabaseInstance(DiscoveryRequest req, StreamObserver<DiscoveryResponse> obs) {
                        discoveryCalls.incrementAndGet();
                        obs.onNext(DiscoveryResponse.newBuilder()
                                .setHost(postgres.getHost())
                                .setPort(postgres.getMappedPort(5432))
                                .setInstanceType(InstanceType.MASTER).build());
                        obs.onCompleted();
                    }
                }).build().start();
    }

    @AfterAll
    static void tearDown() {
        if (grpcServer != null) grpcServer.shutdownNow();
    }

    @Test
    void testDiscoveryAndConnection() throws Exception {
        String url = "jdbc:humus:grpc://localhost:" + GRPC_PORT + "/humus_db";

        try (Connection conn = DriverManager.getConnection(url, "user", "pass");
             Statement stmt = conn.createStatement()) {

            ResultSet rs = stmt.executeQuery("SELECT 1");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
        }
    }

    @Test
    @DisplayName("Check that cached hosts are used")
    void testDiscoveryCaching() throws Exception {
        String url = "jdbc:humus:grpc://localhost:" + GRPC_PORT + "/humus_db";
        Properties props = new Properties();
        props.setProperty("user", "user");
        props.setProperty("password", "pass");

        try (Connection conn1 = DriverManager.getConnection(url, props)) {
            assertNotNull(conn1);
        }

        try (Connection conn2 = DriverManager.getConnection(url, props)) {
            assertNotNull(conn2);
        }

        assertEquals(1, discoveryCalls.get(), "gRPC Discovery должен был вызваться ровно 1 раз");
    }
}
