package dev.humus.core;

import java.sql.SQLException;

public interface ProxyPlugin {
    <T, R> R execute(T target, String methodName, JdbcCallable<T, R> next, Object[] args)
            throws SQLException;
}
