package com.softwareverde.database.mssql;

import com.softwareverde.database.jdbc.JdbcDatabaseConnection;
import com.softwareverde.database.mssql.row.MssqlRowFactory;
import com.softwareverde.database.row.RowFactory;

import java.sql.Connection;

public class MssqlDatabaseConnection extends JdbcDatabaseConnection {

    protected MssqlDatabaseConnection(final Connection connection, final RowFactory rowFactory) {
        super(connection, rowFactory);
    }

    public MssqlDatabaseConnection(final Connection connection) {
        super(connection, new MssqlRowFactory());
    }
}