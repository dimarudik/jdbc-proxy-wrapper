package dev.humus.core;

import java.sql.SQLException;
import java.util.List;

public class PluginChain {
    private final List<ProxyPlugin> plugins;
    private final int index;

    public PluginChain(List<ProxyPlugin> plugins) {
        this(plugins, 0);
    }

    private PluginChain(List<ProxyPlugin> plugins, int index) {
        this.plugins = plugins;
        this.index = index;
    }

    // В PluginChain.java
    public <W, T, R> R proceed(W wrapper, T target, String methodName, JdbcCallable<T, R> terminal, Object[] args)
            throws SQLException {

        if (index >= plugins.size()) {
            return terminal.call(target, args);
        }

        ProxyPlugin currentPlugin = plugins.get(index);
        PluginChain nextChain = new PluginChain(plugins, index + 1);

        // В интерфейс ProxyPlugin нужно будет тоже добавить аргумент 'wrapper'
        return currentPlugin.execute(
                wrapper, // Передаем враппер (ConnectionWrapper)
                target,  // Передаем физический коннект (PgConnection)
                methodName,
                (t, a) -> nextChain.proceed(wrapper, t, methodName, terminal, a),
                args
        );
    }
}
