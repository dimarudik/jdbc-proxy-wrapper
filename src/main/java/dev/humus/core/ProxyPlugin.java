package dev.humus.core;

import java.sql.SQLException;
import java.util.Properties;

public interface ProxyPlugin {
    <W, T, R> R execute(W wrapper, T target, String methodName, JdbcCallable<T, R> next, Object[] args)
            throws SQLException;
    default String getTargetUrl(String url, Properties info) throws SQLException {
        return url;
    }
}
