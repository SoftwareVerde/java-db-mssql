package com.softwareverde.database.mssql.row;


import java.sql.ResultSet;

public interface RowFactory {
    MssqlRow fromResultSet(final ResultSet resultSet);
}