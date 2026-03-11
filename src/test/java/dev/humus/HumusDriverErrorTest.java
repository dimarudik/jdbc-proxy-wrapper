package dev.humus;

import dev.humus.discovery.*;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.*;
import org.slf4j.bridge.SLF4JBridgeHandler;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
public class HumusDriverErrorTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15-alpine")
            .withDatabaseName("humus_db").withUsername("user").withPassword("pass");

    private static Server grpcServer;
    private static final int GRPC_PORT = 9092;
    private static boolean shouldFailDiscovery = false;

    @BeforeAll
    static void setup() throws Exception {
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();

        grpcServer = ServerBuilder.forPort(GRPC_PORT)
                .addService(new DatabaseDiscoveryServiceGrpc.DatabaseDiscoveryServiceImplBase() {
                    @Override
                    public void getDatabaseInstance(DiscoveryRequest req, StreamObserver<DiscoveryResponse> obs) {
                        if (shouldFailDiscovery) {
                            obs.onError(Status.UNAVAILABLE.withDescription("Service Down").asRuntimeException());
                            return;
                        }
                        obs.onNext(DiscoveryResponse.newBuilder()
                                .setHost(postgres.getHost())
                                .setPort(postgres.getMappedPort(5432))
                                .setInstanceType(InstanceType.MASTER).build());
                        obs.onCompleted();
                    }
                }).build().start();
    }

    @AfterAll
    static void tearDown() { grpcServer.shutdownNow(); }

    @BeforeEach
    void reset() {
        shouldFailDiscovery = false;
        GrpcDiscoveryPlugin.invalidateCache("error-cluster");
        GrpcDiscoveryPlugin.invalidateCache("wrong-db-cluster");
        GrpcDiscoveryPlugin.invalidateCache("humus_db");

    }

    @Test
    @DisplayName("Must throw SQLException when discovery service is unavailable")
    void shouldThrowExceptionWhenDiscoveryFails() {
        shouldFailDiscovery = true;
        String url = "jdbc:humus:grpc://localhost:" + GRPC_PORT + "/error-cluster";

        SQLException ex = assertThrows(SQLException.class, () ->
                DriverManager.getConnection(url, "user", "pass")
        );
        assertTrue(ex.getMessage().contains("Discovery service unavailable"));
    }

    @Test
    @DisplayName("Must throw SQLException when database is down")
    void shouldThrowExceptionWhenDatabaseIsDown() {
        shouldFailDiscovery = false;
        shouldFailDiscovery = true;
        String wrongDbCLuster = "/wrong-db-cluster";
        String url = "jdbc:humus:grpc://localhost:" + GRPC_PORT + wrongDbCLuster;

        assertThrows(SQLException.class, () ->
                DriverManager.getConnection(url, "user", "pass")
        );
    }

    @Test
    @DisplayName("Should throw SQLException on SQL errors via StatementWrapper")
    void shouldForwardSqlExceptionsFromStatement() throws SQLException {
        shouldFailDiscovery = false;
        GrpcDiscoveryPlugin.invalidateCache("humus_db");

        String url = "jdbc:humus:grpc://localhost:" + GRPC_PORT + "/humus_db";

        try (Connection conn = DriverManager.getConnection(url, "user", "pass");
             Statement stmt = conn.createStatement()) {

            SQLException ex = assertThrows(SQLException.class, () ->
                    stmt.executeQuery("SELECT * FROM non_existent_table")
            );

            assertFalse(ex.getMessage().contains("Discovery service unavailable"),
                    "Should be SQL error, not Discovery error");
        }
    }

}
