package dev.humus.spi;

import dev.humus.core.JdbcCallable;
import dev.humus.core.ProxyPlugin;

import java.sql.SQLException;
import java.util.Properties;

public class CustomSpiDiscoveryPlugin implements ProxyPlugin {
    private final String mockHost;
    private final int mockPort;

    public CustomSpiDiscoveryPlugin(String host, int port) {
        this.mockHost = host;
        this.mockPort = port;
    }

    @Override
    public String getTargetUrl(String url, Properties info) {
        String dbName = url.substring(url.lastIndexOf("/") + 1);
        return "jdbc:postgresql://" + mockHost + ":" + mockPort + "/" + dbName;
    }

    @Override
    public <W, T, R> R execute(W wrapper, T target, String methodName, JdbcCallable<T, R> next, Object[] args) throws SQLException {
        return next.call(target, args);
    }
}
