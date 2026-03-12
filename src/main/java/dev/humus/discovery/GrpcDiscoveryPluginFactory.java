package dev.humus.discovery;

import dev.humus.core.ProxyPlugin;
import dev.humus.core.ProxyPluginFactory;

import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GrpcDiscoveryPluginFactory implements ProxyPluginFactory {
    private static final Pattern URL_PATTERN = Pattern.compile("jdbc:humus:grpc://([^:/]+):(\\d+)/(.+)");

    @Override
    public ProxyPlugin create(String url, Properties info) {
        Matcher matcher = URL_PATTERN.matcher(url);
        if (matcher.find()) {
            String host = matcher.group(1);
            String port = matcher.group(2);
            String cluster = matcher.group(3);

            return new GrpcDiscoveryPlugin(host + ":" + port, cluster);
        }
        return null;
    }
}
