package com.softwareverde.database.mssql;

import com.softwareverde.database.DatabaseConnection;
import com.softwareverde.database.DatabaseException;
import com.softwareverde.database.Query;
import com.softwareverde.database.Row;
import com.softwareverde.database.mssql.row.MssqlRowFactory;
import com.softwareverde.util.Util;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class MssqlDatabaseConnection implements DatabaseConnection<Connection> {
    private static final String INVALID_ID = "-1";

    private MssqlRowFactory _rowFactory = new MssqlRowFactory();
    private Connection _connection;
    private String _lastInsertId;

    public MssqlDatabaseConnection(Connection connection) {
        _connection = connection;
    }

    @Override
    public synchronized void executeDdl(final String query) throws DatabaseException {
        try {
            if (_connection.isClosed()) {
                throw new DatabaseException("Attempted to execute DDL statement while disconnected.");
            }

            try (final Statement statement = _connection.createStatement()) {
                statement.execute(query);
            }
        }
        catch (final SQLException exception) {
            throw new DatabaseException("Error executing DDL statement.", exception);
        }
    }

    @Override
    public synchronized void executeDdl(final Query query) throws DatabaseException {
        this.executeDdl(query.getQueryString());
    }

    @Override
    public synchronized Long executeSql(final String query, final String[] parameters) throws DatabaseException {
        try {
            if (_connection.isClosed()) {
                throw new DatabaseException("Attempted to execute SQL statement while disconnected.");
            }

            _executeSql(query, parameters);
            return Util.parseLong(_lastInsertId);
        }
        catch (final SQLException exception) {
            throw new DatabaseException("Error executing SQL statement.", exception);
        }
    }

    @Override
    public synchronized Long executeSql(final Query query) throws DatabaseException {
        return this.executeSql(query.getQueryString(), query.getParameters().toArray(new String[0]));
    }

    @Override
    public synchronized List<Row> query(final String query, final String[] parameters) throws DatabaseException {
        try {
            if (_connection.isClosed()) {
                throw new DatabaseException("Attempted to execute query while disconnected.");
            }

            final List<Row> results = new ArrayList<>();
            try (
                    final PreparedStatement preparedStatement = _prepareStatement(query, parameters);
                    final ResultSet resultSet = preparedStatement.executeQuery() ) {
                while (resultSet.next()) {
                    results.add(_rowFactory.fromResultSet(resultSet));
                }
            }
            return results;
        }

        catch (final SQLException exception) {
            throw new DatabaseException("Error executing query.", exception);
        }
    }

    @Override
    public synchronized List<Row> query(final Query query) throws DatabaseException {
        return this.query(query.getQueryString(), query.getParameters().toArray(new String[0]));
    }

    @Override
    public Connection getRawConnection() {
        return _connection;
    }

    @Override
    public void close() throws DatabaseException {
        try {
            _connection.close();
        }
        catch (final SQLException exception) {
            throw new DatabaseException("Unable to close database connection.", exception);
        }
    }

    private String _extractInsertId(final PreparedStatement preparedStatement) throws DatabaseException {
        try (final ResultSet resultSet = preparedStatement.getGeneratedKeys()) {
            final Integer insertId;
            {
                if (resultSet.next()) {
                    insertId = resultSet.getInt(1);
                }
                else {
                    insertId = null;
                }
            }
            return Util.coalesce(insertId, INVALID_ID).toString();
        }
        catch (final SQLException e) {
            throw new DatabaseException("Unable to extract insert ID.", e);
        }
    }

    private PreparedStatement _prepareStatement(final String query, final String[] parameters) throws DatabaseException {
        try {
            final PreparedStatement preparedStatement = _connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
            if (parameters != null) {
                for (int i = 0; i < parameters.length; ++i) {
                    preparedStatement.setString(i+1, parameters[i]);
                }
            }
            return preparedStatement;
        }
        catch (final SQLException e) {
            throw new DatabaseException("Unable to prepare statement.", e);
        }
    }

    private void _executeSql(final String query, final String[] parameters) throws DatabaseException {
        try (final PreparedStatement preparedStatement = _prepareStatement(query, parameters)) {
            preparedStatement.execute();
            _lastInsertId = _extractInsertId(preparedStatement);
        }
        catch (final SQLException e) {
            _lastInsertId = null;
            throw new DatabaseException("Unable to execute query.", e);
        }
    }
}
