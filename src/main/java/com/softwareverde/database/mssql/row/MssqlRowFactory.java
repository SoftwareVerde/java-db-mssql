package com.softwareverde.database.mssql.row;

import java.sql.ResultSet;

public class MssqlRowFactory implements RowFactory {
    public MssqlRow fromResultSet(final ResultSet resultSet) {
        return MssqlRow.fromResultSet(resultSet);
    }
}