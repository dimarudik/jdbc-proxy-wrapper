package dev.humus;

import dev.humus.core.ConnectionWrapper;
import dev.humus.core.ProxyPlugin;
import dev.humus.core.ProxyPluginFactory;

import java.sql.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
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
        if (!acceptsURL(url)) return null;

        List<ProxyPlugin> plugins = new ArrayList<>();
        ServiceLoader<ProxyPluginFactory> loader = ServiceLoader.load(ProxyPluginFactory.class);

        for (ProxyPluginFactory factory : loader) {
            ProxyPlugin plugin = factory.create(url, info);
            if (plugin != null) {
                plugins.add(plugin);
                logger.log(Level.FINE, "Plugin added to chain: {0} (from factory: {1})",
                        new Object[]{plugin.getClass().getName(), factory.getClass().getName()});
            }
        }

        String targetUrl = url;
        for (ProxyPlugin plugin : plugins) {
            String resolved = plugin.getTargetUrl(targetUrl, info);
            if (resolved != null && !resolved.equals(targetUrl)) {
                targetUrl = resolved;
                break;
            }
        }

        if (targetUrl.startsWith(PREFIX)) {
            throw new SQLException("No plugin was able to resolve the target database URL for: " + url);
        }

        Driver underlyingDriver = findUnderlyingDriver(targetUrl);
        Connection physicalConn = underlyingDriver.connect(targetUrl, info);

        return new ConnectionWrapper(physicalConn, plugins, url, info);
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

    private String resolveTargetUrl(List<ProxyPlugin> plugins, String url, Properties info) throws SQLException {
        String currentUrl = url;

        for (ProxyPlugin plugin : plugins) {
            String resolvedUrl = plugin.getTargetUrl(currentUrl, info);
            if (resolvedUrl != null && !resolvedUrl.equals(currentUrl)) {
                logger.log(Level.FINE, "Plugin {0} resolved URL to: {1}",
                        new Object[]{plugin.getClass().getSimpleName(), resolvedUrl});
                currentUrl = resolvedUrl;
            }
        }

        if (currentUrl.startsWith("jdbc:humus:")) {
            throw new SQLException("No plugin was able to resolve the target database URL for: " + url);
        }

        return currentUrl;
    }

    private List<ProxyPlugin> loadPlugins(String url, Properties info) {
        List<ProxyPlugin> plugins = new ArrayList<>();
        ServiceLoader<ProxyPluginFactory> loader = ServiceLoader.load(ProxyPluginFactory.class);
        for (ProxyPluginFactory factory : loader) {
            ProxyPlugin plugin = factory.create(url, info);
            if (plugin != null) {
                plugins.add(plugin);
            }
        }
        return plugins;
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
