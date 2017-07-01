package com.softwareverde.database.mssql.row;

import com.softwareverde.database.Row;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MssqlRow implements Row {
    public static MssqlRow fromResultSet(final ResultSet resultSet) {
        final MssqlRow mssqlRow = new MssqlRow();

        try {
            final ResultSetMetaData metaData = resultSet.getMetaData();
            for (int i = 0; i < metaData.getColumnCount(); ++i ) {
                final String columnName = metaData.getColumnLabel(i+1).toLowerCase(); // metaData.getColumnName(i+1);
                final String columnValue = resultSet.getString(i+1);

                mssqlRow._columnNames.add(columnName);
                mssqlRow._columnValues.put(columnName, columnValue);
            }
        }
        catch (final SQLException e) { }

        return mssqlRow;
    }

    protected List<String> _columnNames = new ArrayList<String>();
    protected Map<String, String> _columnValues = new HashMap<String, String>();

    protected MssqlRow() { }

    @Override
    public List<String> getColumnNames() {
        return new ArrayList<String>(_columnNames);
    }

    @Override
    public String getString(String columnName) {
        return getValue(columnName);
    }

    @Override
    public Integer getInteger(String columnName) {
        return Integer.parseInt(getValue(columnName));
    }

    @Override
    public Long getLong(String columnName) {
        return Long.parseLong(getValue(columnName));
    }

    @Override
    public Float getFloat(String columnName) {
        return Float.parseFloat(getValue(columnName));
    }

    @Override
    public Double getDouble(String columnName) {
        return Double.parseDouble(getValue(columnName));
    }

    @Override
    public Boolean getBoolean(String columnName) {
        String value = getValue(columnName);
        if (value == null) {
            return null;
        }
        try {
            return Integer.parseInt(value) > 0;
        } catch (NumberFormatException e) {
            return value.equalsIgnoreCase("true") || value.equalsIgnoreCase("t") || value.equalsIgnoreCase("yes") || value.equalsIgnoreCase("y");
        }
    }

    @Override
    public byte[] getBytes(String columnName) {
        return getValue(columnName).getBytes();
    }

    private String getValue(final String columnName) {
        final String lowerCaseColumnName = columnName.toLowerCase();
        if (! _columnValues.containsKey(lowerCaseColumnName)) {
            throw new RuntimeException("Row does not contain column: "+ columnName);
        }

        return _columnValues.get(lowerCaseColumnName);
    }
}
