package dev.humus.core;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

public class CallableStatementWrapper extends PreparedStatementWrapper implements CallableStatement {
    private final CallableStatement target;

    public CallableStatementWrapper(CallableStatement target, List<ProxyPlugin> plugins) {
        super(target, plugins);
        this.target = target;
    }

    @Override public void registerOutParameter(int parameterIndex, int sqlType) throws SQLException { target.registerOutParameter(parameterIndex, sqlType); }
    @Override public void registerOutParameter(String parameterName, int sqlType) throws SQLException { target.registerOutParameter(parameterName, sqlType); }

    @Override public String getString(int parameterIndex) throws SQLException { return target.getString(parameterIndex); }
    @Override public boolean getBoolean(int parameterIndex) throws SQLException { return target.getBoolean(parameterIndex); }
    @Override public int getInt(int parameterIndex) throws SQLException { return target.getInt(parameterIndex); }
    @Override public Object getObject(int parameterIndex) throws SQLException { return target.getObject(parameterIndex); }
    @Override public String getString(String parameterName) throws SQLException { return target.getString(parameterName); }
    @Override public boolean getBoolean(String parameterName) throws SQLException { return target.getBoolean(parameterName); }
    @Override public int getInt(String parameterName) throws SQLException { return target.getInt(parameterName); }
    @Override public Object getObject(String parameterName) throws SQLException { return target.getObject(parameterName); }

    @Override public byte getByte(int parameterIndex) throws SQLException { return target.getByte(parameterIndex); }
    @Override public short getShort(int parameterIndex) throws SQLException { return target.getShort(parameterIndex); }
    @Override public long getLong(int parameterIndex) throws SQLException { return target.getLong(parameterIndex); }
    @Override public float getFloat(int parameterIndex) throws SQLException { return target.getFloat(parameterIndex); }
    @Override public double getDouble(int parameterIndex) throws SQLException { return target.getDouble(parameterIndex); }
    @Override public byte[] getBytes(int parameterIndex) throws SQLException { return target.getBytes(parameterIndex); }
    @Override public Date getDate(int parameterIndex) throws SQLException { return target.getDate(parameterIndex); }
    @Override public Time getTime(int parameterIndex) throws SQLException { return target.getTime(parameterIndex); }
    @Override public Timestamp getTimestamp(int parameterIndex) throws SQLException { return target.getTimestamp(parameterIndex); }
    @Override public Object getObject(int parameterIndex, Map<String, Class<?>> map) throws SQLException { return target.getObject(parameterIndex, map); }
    @Override public Ref getRef(int parameterIndex) throws SQLException { return target.getRef(parameterIndex); }
    @Override public Blob getBlob(int parameterIndex) throws SQLException { return target.getBlob(parameterIndex); }
    @Override public Clob getClob(int parameterIndex) throws SQLException { return target.getClob(parameterIndex); }
    @Override public Array getArray(int parameterIndex) throws SQLException { return target.getArray(parameterIndex); }
    @Override public Date getDate(int parameterIndex, Calendar cal) throws SQLException { return target.getDate(parameterIndex, cal); }
    @Override public Time getTime(int parameterIndex, Calendar cal) throws SQLException { return target.getTime(parameterIndex, cal); }
    @Override public Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException { return target.getTimestamp(parameterIndex, cal); }
    @Override public URL getURL(int parameterIndex) throws SQLException { return target.getURL(parameterIndex); }
    @Override public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException { return target.getBigDecimal(parameterIndex, scale); }
    @Override public void registerOutParameter(int parameterIndex, int sqlType, int scale) throws SQLException { target.registerOutParameter(parameterIndex, sqlType, scale); }
    @Override public boolean wasNull() throws SQLException { return target.wasNull();}
    @Override public void registerOutParameter(int parameterIndex, int sqlType, String typeName) throws SQLException { target.registerOutParameter(parameterIndex, sqlType, typeName); }
    @Override public byte getByte(String parameterName) throws SQLException { return target.getByte(parameterName); }
    @Override public short getShort(String parameterName) throws SQLException { return target.getShort(parameterName); }
    @Override public long getLong(String parameterName) throws SQLException { return target.getLong(parameterName); }
    @Override public float getFloat(String parameterName) throws SQLException { return target.getFloat(parameterName); }
    @Override public double getDouble(String parameterName) throws SQLException { return target.getDouble(parameterName); }
    @Override public byte[] getBytes(String parameterName) throws SQLException { return target.getBytes(parameterName); }
    @Override public Date getDate(String parameterName) throws SQLException { return target.getDate(parameterName); }
    @Override public Time getTime(String parameterName) throws SQLException { return target.getTime(parameterName); }
    @Override public Timestamp getTimestamp(String parameterName) throws SQLException { return target.getTimestamp(parameterName); }
    @Override public Object getObject(String parameterName, Map<String, Class<?>> map) throws SQLException { return target.getObject(parameterName, map); }
    @Override public Ref getRef(String parameterName) throws SQLException { return target.getRef(parameterName); }
    @Override public Blob getBlob(String parameterName) throws SQLException { return target.getBlob(parameterName); }
    @Override public Clob getClob(String parameterName) throws SQLException { return target.getClob(parameterName); }
    @Override public Array getArray(String parameterName) throws SQLException { return target.getArray(parameterName); }
    @Override public Date getDate(String parameterName, Calendar cal) throws SQLException { return target.getDate(parameterName, cal); }
    @Override public Time getTime(String parameterName, Calendar cal) throws SQLException { return target.getTime(parameterName, cal); }
    @Override public Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException { return target.getTimestamp(parameterName, cal); }
    @Override public URL getURL(String parameterName) throws SQLException { return target.getURL(parameterName); }
    @Override public void setURL(String parameterName, URL val) throws SQLException { target.setURL(parameterName, val); }
    @Override public void setNull(String parameterName, int sqlType) throws SQLException { target.setNull(parameterName, sqlType); }
    @Override public void setBoolean(String parameterName, boolean x) throws SQLException { target.setBoolean(parameterName, x); }
    @Override public void setByte(String parameterName, byte x) throws SQLException { target.setByte(parameterName, x); }
    @Override public void setShort(String parameterName, short x) throws SQLException { target.setShort(parameterName, x); }
    @Override public void setInt(String parameterName, int x) throws SQLException { target.setInt(parameterName, x); }
    @Override public void setLong(String parameterName, long x) throws SQLException { target.setLong(parameterName, x); }
    @Override public void setFloat(String parameterName, float x) throws SQLException { target.setFloat(parameterName, x); }
    @Override public void setDouble(String parameterName, double x) throws SQLException { target.setDouble(parameterName, x); }
    @Override public void setBigDecimal(String parameterName, BigDecimal x) throws SQLException { target.setBigDecimal(parameterName, x); }
    @Override public void setString(String parameterName, String x) throws SQLException { target.setString(parameterName, x); }
    @Override public void setBytes(String parameterName, byte[] x) throws SQLException { target.setBytes(parameterName, x); }
    @Override public void setDate(String parameterName, Date x) throws SQLException { target.setDate(parameterName, x); }
    @Override public void setTime(String parameterName, Time x) throws SQLException { target.setTime(parameterName, x); }
    @Override public void setTimestamp(String parameterName, Timestamp x) throws SQLException { target.setTimestamp(parameterName, x); }
    @Override public void setAsciiStream(String parameterName, InputStream x, int length) throws SQLException { target.setAsciiStream(parameterName, x, length); }
    @Override public void setBinaryStream(String parameterName, InputStream x, int length) throws SQLException { target.setBinaryStream(parameterName, x, length); }
    @Override public void setObject(String parameterName, Object x, int targetSqlType, int scale) throws SQLException { target.setObject(parameterName, x, targetSqlType, scale); }
    @Override public void setObject(String parameterName, Object x, int targetSqlType) throws SQLException { target.setObject(parameterName, x, targetSqlType); }
    @Override public void setObject(String parameterName, Object x) throws SQLException { target.setObject(parameterName, x); }
    @Override public void setCharacterStream(String parameterName, Reader reader, int length) throws SQLException { target.setCharacterStream(parameterName, reader, length); }
    @Override public void setDate(String parameterName, Date x, Calendar cal) throws SQLException { target.setDate(parameterName, x, cal); }
    @Override public void setTime(String parameterName, Time x, Calendar cal) throws SQLException { target.setTime(parameterName, x, cal); }
    @Override public void setTimestamp(String parameterName, Timestamp x, Calendar cal) throws SQLException { target.setTimestamp(parameterName, x, cal); }
    @Override public void setNull(String parameterName, int sqlType, String typeName) throws SQLException { target.setNull(parameterName, sqlType, typeName); }
    @Override public BigDecimal getBigDecimal(int parameterIndex) throws SQLException { return target.getBigDecimal(parameterIndex); }
    @Override public BigDecimal getBigDecimal(String parameterName) throws SQLException { return target.getBigDecimal(parameterName); }
    @Override public void registerOutParameter(String parameterName, int sqlType, int scale) throws SQLException { target.registerOutParameter(parameterName, sqlType, scale); }
    @Override public void registerOutParameter(String parameterName, int sqlType, String typeName) throws SQLException { target.registerOutParameter(parameterName, sqlType, typeName); }
    @Override public RowId getRowId(int parameterIndex) throws SQLException { return target.getRowId(parameterIndex); }
    @Override public RowId getRowId(String parameterName) throws SQLException { return target.getRowId(parameterName); }
    @Override public void setRowId(String parameterName, RowId x) throws SQLException { target.setRowId(parameterName, x); }
    @Override public void setNString(String parameterName, String value) throws SQLException { target.setNString(parameterName, value); }
    @Override public void setNCharacterStream(String parameterName, Reader value, long length) throws SQLException { target.setNCharacterStream(parameterName, value, length); }
    @Override public void setNClob(String parameterName, NClob value) throws SQLException { target.setNClob(parameterName, value); }
    @Override public void setClob(String parameterName, Reader reader, long length) throws SQLException { target.setClob(parameterName, reader, length); }
    @Override public void setBlob(String parameterName, InputStream inputStream, long length) throws SQLException { target.setBlob(parameterName, inputStream, length); }
    @Override public void setNClob(String parameterName, Reader reader, long length) throws SQLException { target.setNClob(parameterName, reader, length); }
    @Override public NClob getNClob(int parameterIndex) throws SQLException { return target.getNClob(parameterIndex); }
    @Override public NClob getNClob(String parameterName) throws SQLException { return target.getNClob(parameterName); }
    @Override public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException { target.setSQLXML(parameterName, xmlObject); }
    @Override public SQLXML getSQLXML(int parameterIndex) throws SQLException { return target.getSQLXML(parameterIndex); }
    @Override public SQLXML getSQLXML(String parameterName) throws SQLException { return target.getSQLXML(parameterName); }
    @Override public String getNString(int parameterIndex) throws SQLException { return target.getNString(parameterIndex); }
    @Override public String getNString(String parameterName) throws SQLException { return target.getNString(parameterName); }
    @Override public Reader getNCharacterStream(int parameterIndex) throws SQLException { return target.getNCharacterStream(parameterIndex); }
    @Override public Reader getNCharacterStream(String parameterName) throws SQLException { return target.getNCharacterStream(parameterName); }
    @Override public Reader getCharacterStream(int parameterIndex) throws SQLException { return target.getCharacterStream(parameterIndex); }
    @Override public Reader getCharacterStream(String parameterName) throws SQLException { return target.getCharacterStream(parameterName); }
    @Override public void setBlob(String parameterName, Blob x) throws SQLException { target.setBlob(parameterName, x); }
    @Override public void setClob(String parameterName, Clob x) throws SQLException { target.setClob(parameterName, x); }
    @Override public void setAsciiStream(String parameterName, InputStream x, long length) throws SQLException { target.setAsciiStream(parameterName, x, length); }
    @Override public void setBinaryStream(String parameterName, InputStream x, long length) throws SQLException { target.setBinaryStream(parameterName, x, length); }
    @Override public void setCharacterStream(String parameterName, Reader reader, long length) throws SQLException { target.setCharacterStream(parameterName, reader, length); }
    @Override public void setAsciiStream(String parameterName, InputStream x) throws SQLException { target.setAsciiStream(parameterName, x); }
    @Override public void setBinaryStream(String parameterName, InputStream x) throws SQLException { target.setBinaryStream(parameterName, x); }
    @Override public void setCharacterStream(String parameterName, Reader reader) throws SQLException { target.setCharacterStream(parameterName, reader); }
    @Override public void setNCharacterStream(String parameterName, Reader value) throws SQLException { target.setNCharacterStream(parameterName, value); }
    @Override public void setClob(String parameterName, Reader reader) throws SQLException { target.setClob(parameterName, reader); }
    @Override public void setBlob(String parameterName, InputStream inputStream) throws SQLException { target.setBlob(parameterName, inputStream); }
    @Override public void setNClob(String parameterName, Reader reader) throws SQLException { target.setNClob(parameterName, reader); }
    @Override public <T> T getObject(int parameterIndex, Class<T> type) throws SQLException { return target.getObject(parameterIndex, type); }
    @Override public <T> T getObject(String parameterName, Class<T> type) throws SQLException { return target.getObject(parameterName, type); }
}
