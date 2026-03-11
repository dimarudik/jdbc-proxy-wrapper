package dev.humus.core;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.List;

public class PreparedStatementWrapper extends StatementWrapper implements PreparedStatement {
    private final PreparedStatement target;
    private final List<ProxyPlugin> plugins;

    public PreparedStatementWrapper(PreparedStatement target, List<ProxyPlugin> plugins) {
        super(target, plugins);
        this.target = target;
        this.plugins = plugins;
    }

    private <R> R execute(String methodName, JdbcCallable<PreparedStatement, R> terminal, Object[] args) throws SQLException {
        return new PluginChain(plugins).proceed(this, target, methodName, terminal, args);
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        return execute("executeQuery", (t, a) -> t.executeQuery(), null);
    }

    @Override
    public int executeUpdate() throws SQLException {
        return execute("executeUpdate", (t, a) -> t.executeUpdate(), null);
    }

    @Override
    public boolean execute() throws SQLException {
        return execute("execute", (t, a) -> t.execute(), null);
    }

    @Override public void setNull(int parameterIndex, int sqlType) throws SQLException { target.setNull(parameterIndex, sqlType); }
    @Override public void setBoolean(int parameterIndex, boolean x) throws SQLException { target.setBoolean(parameterIndex, x); }
    @Override public void setInt(int parameterIndex, int x) throws SQLException { target.setInt(parameterIndex, x); }
    @Override public void setLong(int parameterIndex, long x) throws SQLException { target.setLong(parameterIndex, x); }
    @Override public void setString(int parameterIndex, String x) throws SQLException { target.setString(parameterIndex, x); }
    @Override public void setObject(int parameterIndex, Object x) throws SQLException { target.setObject(parameterIndex, x); }
    @Override public void clearParameters() throws SQLException { target.clearParameters(); }
    @Override public void addBatch() throws SQLException { target.addBatch(); }
    @Override public ResultSetMetaData getMetaData() throws SQLException { return target.getMetaData(); }
    @Override public ParameterMetaData getParameterMetaData() throws SQLException { return target.getParameterMetaData(); }
    @Override public void setByte(int parameterIndex, byte x) throws SQLException { target.setByte(parameterIndex, x); }
    @Override public void setShort(int parameterIndex, short x) throws SQLException { target.setShort(parameterIndex, x); }
    @Override public void setFloat(int parameterIndex, float x) throws SQLException { target.setFloat(parameterIndex, x); }
    @Override public void setDouble(int parameterIndex, double x) throws SQLException { target.setDouble(parameterIndex, x); }
    @Override public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException { target.setBigDecimal(parameterIndex, x); }
    @Override public void setBytes(int parameterIndex, byte[] x) throws SQLException { target.setBytes(parameterIndex, x); }
    @Override public void setDate(int parameterIndex, Date x) throws SQLException { target.setDate(parameterIndex, x); }
    @Override public void setTime(int parameterIndex, Time x) throws SQLException { target.setTime(parameterIndex, x); }
    @Override public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException { target.setTimestamp(parameterIndex, x); }
    @Override public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException { target.setAsciiStream(parameterIndex, x, length); }
    @Override public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException { target.setBinaryStream(parameterIndex, x, length); }
    @Override public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException { target.setCharacterStream(parameterIndex, reader, length);}
    @Override public void setRef(int parameterIndex, Ref x) throws SQLException { target.setRef(parameterIndex, x); }
    @Override public void setBlob(int parameterIndex, Blob x) throws SQLException { target.setBlob(parameterIndex, x); }
    @Override public void setClob(int parameterIndex, Clob x) throws SQLException { target.setClob(parameterIndex, x); }
    @Override public void setArray(int parameterIndex, Array x) throws SQLException { target.setArray(parameterIndex, x); }
    @Override public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException { target.setDate(parameterIndex, x, cal); }
    @Override public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException { target.setTime(parameterIndex, x, cal); }
    @Override public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException { target.setTimestamp(parameterIndex, x, cal); }
    @Override public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException { target.setNull(parameterIndex, sqlType, typeName); }
    @Override public void setURL(int parameterIndex, URL x) throws SQLException { target.setURL(parameterIndex, x); }
    @Override public void setRowId(int parameterIndex, RowId x) throws SQLException { target.setRowId(parameterIndex, x); }
    @Override public void setNString(int parameterIndex, String value) throws SQLException { target.setNString(parameterIndex, value); }
    @Override public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException { target.setNCharacterStream(parameterIndex, value, length); }
    @Override public void setNClob(int parameterIndex, NClob value) throws SQLException { target.setNClob(parameterIndex, value); }
    @Override public void setClob(int parameterIndex, Reader reader, long length) throws SQLException { target.setClob(parameterIndex, reader, length); }
    @Override public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException { target.setBlob(parameterIndex, inputStream, length); }
    @Override public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException { target.setNClob(parameterIndex, reader, length); }
    @Override public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException { target.setSQLXML(parameterIndex, xmlObject);}
    @Override public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException { target.setObject(parameterIndex, x, targetSqlType); }
    @Override public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException { target.setObject(parameterIndex, x, targetSqlType, scaleOrLength); }
    @Override public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException { target.setAsciiStream(parameterIndex, x, length); }
    @Override public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException { target.setBinaryStream(parameterIndex, x, length); }
    @Override public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException { target.setAsciiStream(parameterIndex, x);}
    @Override public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException { target.setBinaryStream(parameterIndex, x);}
    @Override public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {target.setCharacterStream(parameterIndex, reader);}
    @Override public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {target.setNCharacterStream(parameterIndex, value);}
    @Override public void setClob(int parameterIndex, Reader reader) throws SQLException {target.setClob(parameterIndex, reader);}
    @Override public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {target.setBlob(parameterIndex, inputStream);}
    @Override public void setNClob(int parameterIndex, Reader reader) throws SQLException {target.setNClob(parameterIndex, reader);}
    @Override public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {target.setCharacterStream(parameterIndex, reader, length);}
    @Override @Deprecated public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException { target.setUnicodeStream(parameterIndex, x, length);}
}
