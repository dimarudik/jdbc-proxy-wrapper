package dev.humus.core;

import java.sql.SQLException;
import java.util.Properties;

public interface ConnectionProvider {
    String getTargetUrl(String originalUrl, Properties info) throws SQLException;
}
