package com.softwareverde.database.mssql.row;

import com.softwareverde.database.jdbc.row.JdbcRow;

public class MssqlRow extends JdbcRow {
    protected MssqlRow(final JdbcRow jdbcRow) {
        super(jdbcRow);
    }

    public MssqlRow() { }
}