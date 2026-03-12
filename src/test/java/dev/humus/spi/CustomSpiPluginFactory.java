package dev.humus.spi;

import dev.humus.core.ProxyPlugin;
import dev.humus.core.ProxyPluginFactory;

import java.util.Properties;

public class CustomSpiPluginFactory implements ProxyPluginFactory {
    @Override
    public ProxyPlugin create(String url, Properties info) {
        if (url.startsWith("jdbc:humus:custom-spi://")) {
            String host = System.getProperty("test.postgres.host", "127.0.0.1");
            int port = Integer.parseInt(System.getProperty("test.postgres.port", "5432"));

            return new CustomSpiDiscoveryPlugin(host, port);
        }
        return null;
    }
}
