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

        String host = matcher.group(1);
        int port = Integer.parseInt(matcher.group(2));
        String clusterName = matcher.group(3);

        String discoveryAddr = host + ":" + port;

        GrpcDiscoveryPlugin plugin = new GrpcDiscoveryPlugin(discoveryAddr, clusterName);

        DiscoveryResponse response = plugin.resolve();

        String realJdbcUrl = String.format("jdbc:postgresql://%s:%d/%s",
                response.getHost(), response.getPort(), clusterName);

        Connection physicalConn = DriverManager.getConnection(realJdbcUrl, info);

        return new ConnectionWrapper(physicalConn, List.of(plugin), url, info);
    }

    @Override
    public boolean acceptsURL(String url) {
        return url != null && url.startsWith(PREFIX);
    }

    @Override public int getMajorVersion() { return 1; }
    @Override public int getMinorVersion() { return 0; }
    @Override public boolean jdbcCompliant() { return false; }
    @Override public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) { return new DriverPropertyInfo[0]; }
    @Override public java.util.logging.Logger getParentLogger() { return java.util.logging.Logger.getLogger("dev.humus"); }
}
