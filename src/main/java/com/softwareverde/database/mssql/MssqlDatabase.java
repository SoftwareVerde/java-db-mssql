package com.softwareverde.database.mssql;

import com.softwareverde.database.Database;
import com.softwareverde.database.DatabaseConnection;
import com.softwareverde.database.DatabaseException;
import com.softwareverde.database.Row;
import com.softwareverde.database.mssql.row.MssqlRowFactory;
import com.softwareverde.database.mssql.row.RowFactory;
import com.softwareverde.util.Util;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class MssqlDatabase implements Database<Connection> {
    private RowFactory _rowFactory = new MssqlRowFactory();
    private String _username = "root";
    private String _password = "";
    private String _url = "localhost";
    private String _port = "1433";
    private String _databaseName = "";

    private Connection _connect() throws ClassNotFoundException, SQLException {
        final String connectionString = "jdbc:sqlserver://"+ _url +":"+ _port +";databaseName="+ _databaseName;
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        return DriverManager.getConnection(connectionString, _username, _password);
    }

    public MssqlDatabase(final String url, final String username, final String password) {
        _url = url;
        _username = username;
        _password = password;
    }

    public MssqlDatabase(final String url, final String username, final String password, final String port) {
        _url = url;
        _username = username;
        _password = password;
        _port = port;
    }

    public void setDatabase(final String databaseName) {
        _databaseName = databaseName;
    }

    @Override
    public DatabaseConnection<Connection> newConnection() throws DatabaseException {
        try {
            Connection connection = _connect();
            return new MssqlDatabaseConnection(connection);
        } catch (Exception e) {
            throw new DatabaseException("Unable to create MSSQL connection.", e);
        }
    }

    /**
     * Require dependencies be packaged at compile-time.
     */
    private static final Class[] UNUSED = {
        com.microsoft.sqlserver.jdbc.SQLServerDriver.class
    };
}
