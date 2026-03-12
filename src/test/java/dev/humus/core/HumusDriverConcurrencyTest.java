package dev.humus.core;

import dev.humus.discovery.*;
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
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
public class HumusDriverConcurrencyTest {
    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres")
            .withDatabaseName("humus_db").withUsername("user").withPassword("pass");

    private static Server grpcServer;
    private static final int GRPC_PORT = 9093;
    private static final AtomicInteger grpcCallCount = new AtomicInteger(0);

    @BeforeAll
    static void setup() throws Exception {
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();

        grpcServer = ServerBuilder.forPort(GRPC_PORT)
                .addService(new DatabaseDiscoveryServiceGrpc.DatabaseDiscoveryServiceImplBase() {
                    @Override
                    public void getDatabaseInstance(DiscoveryRequest req, StreamObserver<DiscoveryResponse> obs) {
                        grpcCallCount.incrementAndGet();
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

    @Test
    @DisplayName("Should handle 50 concurrent connections")
    void shouldHandleConcurrentConnections() throws InterruptedException {
        int threadCount = 50;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);

        Set<Throwable> errors = Collections.newSetFromMap(new ConcurrentHashMap<>());
        String url = "jdbc:humus:grpc://localhost:" + GRPC_PORT + "/humus_db";

        GrpcDiscoveryPlugin.invalidateCache("humus_db");
        grpcCallCount.set(0);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    latch.await();
                    try (Connection ignored = DriverManager.getConnection(url, "user", "pass")) {
                        // Connection is successful
                    }
                } catch (Throwable t) {
                    errors.add(t);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        latch.countDown();
        doneLatch.await();
        executor.shutdown();

        assertEquals(0, errors.size(), "No exceptions should have been thrown: " + errors);

        System.out.println("Total gRPC calls: " + grpcCallCount.get());
    }
}
