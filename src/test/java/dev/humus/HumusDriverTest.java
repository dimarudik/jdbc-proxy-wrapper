package dev.humus;

import dev.humus.core.ConnectionWrapper;
import dev.humus.discovery.DatabaseDiscoveryServiceGrpc;
import dev.humus.discovery.DiscoveryRequest;
import dev.humus.discovery.DiscoveryResponse;
import dev.humus.discovery.InstanceType;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    void testSimpleDiscovery() throws Exception {
        String url = "jdbc:humus:grpc://localhost:9091/humus_db";
        try (Connection conn = DriverManager.getConnection(url, "user", "pass")) {
            assertTrue(conn instanceof ConnectionWrapper);

            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("SELECT 1");
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
            }
        }
    }
}
