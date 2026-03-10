package dev.humus.core;

import java.sql.SQLException;

@FunctionalInterface
public interface JdbcCallable<T, R> {
    R call(T target, Object[] args) throws SQLException;
}
