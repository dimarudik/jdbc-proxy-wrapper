package dev.humus.spi;

import dev.humus.core.ConnectionWrapper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.bridge.SLF4JBridgeHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.sql.Connection;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@Testcontainers
public class HumusSpringSpiTest {
    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15-alpine")
            .withDatabaseName("spring_spi_db").withUsername("user").withPassword("pass");

    @BeforeAll
    static void setupLogging() {
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();
        java.util.logging.Logger.getLogger("dev.humus").setLevel(java.util.logging.Level.FINEST);
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        System.setProperty("test.postgres.host", postgres.getHost());
        System.setProperty("test.postgres.port", String.valueOf(postgres.getMappedPort(5432)));

        registry.add("spring.datasource.url", () -> "jdbc:humus:custom-spi://mock-host:0/spring_spi_db");
        registry.add("spring.datasource.driver-class-name", () -> "dev.humus.HumusDriver");
        registry.add("spring.datasource.username", () -> "user");
        registry.add("spring.datasource.password", () -> "pass");
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "create-drop");
    }

    @Autowired
    private DataSource dataSource;

    @Test
    @DisplayName("Spring Boot should successfully create a connection via a custom SPI plugin")
    void testSpringSpiIntegration() throws Exception {
        try (Connection conn = dataSource.getConnection()) {
            assertNotNull(conn);
            assertTrue(conn.isWrapperFor(ConnectionWrapper.class) || conn instanceof ConnectionWrapper);

            String resolvedUrl = conn.getMetaData().getURL();
            assertTrue(resolvedUrl.contains(postgres.getHost()), "URL should contain the actual host of container");

            System.out.println("Spring Boot SPI test passed. Resolved URL: " + resolvedUrl);
        }
    }

    @SpringBootApplication
    static class TestApp {}
}
