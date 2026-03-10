package dev.humus;

import dev.humus.core.ConnectionWrapper;
import dev.humus.discovery.DiscoveryResponse;
import dev.humus.discovery.GrpcDiscoveryPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class HumusDriver implements Driver {
    private static final Logger log = LoggerFactory.getLogger(HumusDriver.class);
    private static final String PREFIX = "jdbc:humus:";
    private static final Pattern URL_PATTERN = Pattern.compile("jdbc:humus:grpc://([^:/]+):(\\d+)/(.+)");

    static {
        try {
            DriverManager.registerDriver(new HumusDriver());
        } catch (SQLException e) {
            log.error("Failed to register HumusDriver", e);
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) return null;

        Matcher matcher = URL_PATTERN.matcher(url);
        if (!matcher.find()) {
            throw new SQLException("Invalid URL. Expected: jdbc:humus:grpc://host:port/clusterName");
        }

        String grpcHost = matcher.group(1);
        int grpcPort = Integer.parseInt(matcher.group(2));
        String clusterName = matcher.group(3);

        // 1. Инициализируем Discovery-плагин
        GrpcDiscoveryPlugin discoveryPlugin = new GrpcDiscoveryPlugin(grpcHost + ":" + grpcPort, clusterName);

        // 2. Делаем первый resolve, чтобы узнать, куда коннектиться изначально
        DiscoveryResponse node = discoveryPlugin.resolve();

        // 3. Формируем реальный URL для целевого драйвера (PostgreSQL)
        String targetUrl = String.format("jdbc:postgresql://%s:%d/%s",
                node.getHost(), node.getPort(), clusterName);

        log.debug("Connecting to target: {} ({})", targetUrl, node.getInstanceType());

        Connection physicalConn = DriverManager.getConnection(targetUrl, info);
        return new ConnectionWrapper(physicalConn, List.of(discoveryPlugin), url, info);
    }

    @Override
    public boolean acceptsURL(String url) {
        return url != null && url.startsWith(PREFIX);
    }

    // ... остальные стандартные методы JDBC Driver (version, compliance и т.д.)
    @Override public int getMajorVersion() { return 1; }
    @Override public int getMinorVersion() { return 0; }
    @Override public boolean jdbcCompliant() { return false; }
    @Override public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) { return new DriverPropertyInfo[0]; }
    @Override public java.util.logging.Logger getParentLogger() { return java.util.logging.Logger.getLogger("dev.humus"); }
}
