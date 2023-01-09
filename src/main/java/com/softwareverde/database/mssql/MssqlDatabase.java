package com.softwareverde.database.mssql;

import com.softwareverde.database.jdbc.JdbcDatabase;
import com.softwareverde.database.properties.DatabaseCredentials;
import com.softwareverde.database.properties.DatabaseProperties;

import java.util.Properties;

public class MssqlDatabase extends MssqlDatabaseConnectionFactory implements JdbcDatabase {
    public static final Integer DEFAULT_PORT = 1433;

    public MssqlDatabase(final DatabaseProperties databaseProperties) {
        this(databaseProperties, databaseProperties.getCredentials());
    }

    public MssqlDatabase(final DatabaseProperties databaseProperties, final DatabaseCredentials credentials) {
        super(databaseProperties.getHostname(), databaseProperties.getPort(), databaseProperties.getSchema(), credentials.username, credentials.password);
    }

    public MssqlDatabase(final String host, final Integer port, final String username, final String password) {
        super(host, port, "", username, password);
    }

    public MssqlDatabase(final String host, final Integer port, final String username, final String password, final Properties properties) {
        super(host, port, "", username, password, properties);
    }

    public void setSchema(final String schema) {
        _schema = schema;
    }

    public MssqlDatabaseConnectionFactory newConnectionFactory() {
        return new MssqlDatabaseConnectionFactory(_hostname, _port, _schema, _username, _password, _connectionProperties);
    }
}