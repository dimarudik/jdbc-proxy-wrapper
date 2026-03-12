package dev.humus.spi;

import dev.humus.core.ConnectionWrapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
public class HumusSpiIntegrationTest {
    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15-alpine")
            .withDatabaseName("spi_db").withUsername("user").withPassword("pass");

    @Test
    @DisplayName("Должен найти и использовать кастомный плагин через Java SPI")
    void shouldLoadCustomPluginViaSpi() throws Exception {
        // Регистрируем драйвер
        Class.forName("dev.humus.HumusDriver");

        // Формируем URL, который понимает ТОЛЬКО наша новая CustomSpiPluginFactory
        // Мы передаем реальные порты контейнера, чтобы проверка прошла до конца
        String customUrl = "jdbc:humus:custom-spi://ignored-host:0/spi_db";

        Properties props = new Properties();
        props.setProperty("user", "user");
        props.setProperty("password", "pass");

        // ВАЖНО: Нам нужно, чтобы CustomSpiDiscoveryPlugin вернул правильный адрес контейнера
        // Для этого можно передать параметры в Properties, которые плагин прочитает
        // Но для теста SPI нам достаточно увидеть, что Connection создался.

        // Чтобы тест был честным, обновим CustomSpiPluginFactory или передадим параметры:
        System.setProperty("test.postgres.host", postgres.getHost());
        System.setProperty("test.postgres.port", String.valueOf(postgres.getMappedPort(5432)));

        try (Connection conn = DriverManager.getConnection(customUrl, props)) {
            assertNotNull(conn);
            assertTrue(conn instanceof ConnectionWrapper);

            // Проверяем, что метаданные указывают на реальный Postgres,
            // хотя в URL прокси мы писали "ignored-host"
            String realUrl = conn.getMetaData().getURL();
            assertTrue(realUrl.contains(postgres.getHost()));

            System.out.println("SPI Custom Plugin successfully resolved URL to: " + realUrl);
        }
    }
}
