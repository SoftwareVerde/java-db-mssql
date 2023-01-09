package com.softwareverde.database.mssql.row;

import com.softwareverde.database.DatabaseException;
import com.softwareverde.database.jdbc.row.JdbcRow;
import com.softwareverde.database.jdbc.row.JdbcRowFactory;

import java.sql.ResultSet;

public class MssqlRowFactory extends JdbcRowFactory {
    public MssqlRow fromResultSet(final ResultSet resultSet) throws DatabaseException {
        final JdbcRow jdbcRow = super.fromResultSet(resultSet);
        return new MssqlRow(jdbcRow);
    }
}