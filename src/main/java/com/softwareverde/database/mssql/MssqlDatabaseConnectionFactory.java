package com.softwareverde.database.mssql;

import com.softwareverde.database.DatabaseException;
import com.softwareverde.database.jdbc.JdbcDatabaseConnectionFactory;
import com.softwareverde.database.properties.DatabaseCredentials;
import com.softwareverde.database.properties.DatabaseProperties;
import com.softwareverde.util.Util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

public class MssqlDatabaseConnectionFactory  implements JdbcDatabaseConnectionFactory {
    public static String createConnectionString(final String hostname, final Integer port, final String schema) {
        final StringBuilder stringBuilder = new StringBuilder("jdbc:sqlserver://");
        stringBuilder.append(Util.coalesce(hostname, "localhost"));
        stringBuilder.append(":");
        stringBuilder.append(Util.coalesce(port, MssqlDatabase.DEFAULT_PORT));
        stringBuilder.append(";databaseName=");
        stringBuilder.append(Util.coalesce(schema, ""));

        return stringBuilder.toString();
    }

    protected static void _setDefaultConnectionProperties(final Properties properties) {
        if (! properties.containsKey("allowPublicKeyRetrieval")) {
            properties.setProperty("allowPublicKeyRetrieval", "true");
        }

        if (! properties.containsKey("useSSL")) {
            properties.setProperty("useSSL", "false");
        }

        if (! properties.containsKey("serverTimezone")) {
            properties.setProperty("serverTimezone", "UTC");
        }
    }

    /**
     * Require dependencies be packaged at compile-time.
     */
    private static final Class[] UNUSED = {
        com.microsoft.sqlserver.jdbc.SQLServerDriver.class
    };

    protected final String _hostname;
    protected final Integer _port;
    protected final Properties _connectionProperties;

    protected String _username;
    protected String _password;
    protected String _schema;

    public MssqlDatabaseConnectionFactory(final DatabaseProperties databaseProperties) {
        this(databaseProperties, databaseProperties.getCredentials());
    }

    public MssqlDatabaseConnectionFactory(final DatabaseProperties databaseProperties, final DatabaseCredentials databaseCredentials) {
        this(databaseProperties, databaseCredentials, new Properties());
    }

    public MssqlDatabaseConnectionFactory(final DatabaseProperties databaseProperties, final DatabaseCredentials databaseCredentials, final Properties connectionProperties) {
        this(databaseProperties.getHostname(), databaseProperties.getPort(), databaseProperties.getSchema(), databaseCredentials.username, databaseCredentials.password, connectionProperties);
    }

    public MssqlDatabaseConnectionFactory(final String hostname, final Integer port, final String schema, final String username, final String password) {
        this(hostname, port, schema, username, password, new Properties());
    }

    public MssqlDatabaseConnectionFactory(final String hostname, final Integer port, final String schema, final String username, final String password, final Properties properties) {
        _hostname = hostname;
        _port = port;
        _schema = schema;
        _connectionProperties = new Properties(properties);

        _username = username;
        _password = password;

        MssqlDatabaseConnectionFactory._setDefaultConnectionProperties(_connectionProperties);
    }

    @Override
    public MssqlDatabaseConnection newConnection() throws DatabaseException {
        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            final String connectionString = MssqlDatabaseConnectionFactory.createConnectionString(_hostname, _port, _schema);

            final Properties connectionProperties = new Properties(_connectionProperties);
            connectionProperties.put("user", _username);
            connectionProperties.put("password", _password);

            final Connection connection = DriverManager.getConnection(connectionString, connectionProperties);
            return new MssqlDatabaseConnection(connection);
        }
        catch (final Exception exception) {
            throw new DatabaseException(exception);
        }
    }
}