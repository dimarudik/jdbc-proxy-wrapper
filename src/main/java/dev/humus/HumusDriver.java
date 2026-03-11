package dev.humus;

import dev.humus.core.ConnectionWrapper;
import dev.humus.discovery.DiscoveryResponse;
import dev.humus.discovery.GrpcDiscoveryPlugin;

import java.sql.*;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class HumusDriver implements Driver {
    private static final Logger logger = Logger.getLogger(HumusDriver.class.getName());

    private static final String PREFIX = "jdbc:humus:";
    private static final Pattern URL_PATTERN = Pattern.compile("jdbc:humus:grpc://([^:/]+):(\\d+)/(.+)");

    static {
        try {
            DriverManager.registerDriver(new HumusDriver());
        } catch (SQLException e) {
            logger.log(Level.SEVERE, "Failed to register HumusDriver", e);
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }

        Matcher matcher = URL_PATTERN.matcher(url);
        if (!matcher.find()) {
            throw new SQLException("Invalid Humus URL format. Expected: jdbc:humus:grpc://host:port/clusterName");
        }

        String discoveryHost = matcher.group(1);
        String discoveryPort = matcher.group(2);
        String clusterName = matcher.group(3);

        GrpcDiscoveryPlugin discoveryPlugin = new GrpcDiscoveryPlugin(
                discoveryHost + ":" + discoveryPort, clusterName);

        DiscoveryResponse node = discoveryPlugin.resolve();

        String targetUrl = String.format("jdbc:postgresql://%s:%d/%s",
                node.getHost(), node.getPort(), clusterName);

        logger.log(Level.FINE, "Resolved target URL: {0}", targetUrl);

        Driver underlyingDriver = findUnderlyingDriver(targetUrl);
        Connection physicalConn = underlyingDriver.connect(targetUrl, info);

        if (physicalConn == null) {
            throw new SQLException("Underlying driver failed to return a connection for " + targetUrl);
        }

        return new ConnectionWrapper(physicalConn, Collections.singletonList(discoveryPlugin), url, info);
    }

    private Driver findUnderlyingDriver(String url) throws SQLException {
        Enumeration<Driver> drivers = DriverManager.getDrivers();
        while (drivers.hasMoreElements()) {
            Driver driver = drivers.nextElement();
            if (!(driver instanceof HumusDriver) && driver.acceptsURL(url)) {
                return driver;
            }
        }
        throw new SQLException("No suitable underlying driver found for " + url);
    }

    @Override
    public boolean acceptsURL(String url) {
        return url != null && url.startsWith(PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() { return 1; }

    @Override
    public int getMinorVersion() { return 0; }

    @Override
    public boolean jdbcCompliant() { return false; }

    @Override
    public Logger getParentLogger() {
        return Logger.getLogger("dev.humus");
    }
}
