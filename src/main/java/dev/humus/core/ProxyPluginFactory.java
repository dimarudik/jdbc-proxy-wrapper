package dev.humus.core;

import java.util.Properties;

public interface ProxyPluginFactory {
    ProxyPlugin create(String url, Properties info);
}
