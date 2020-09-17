/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import io.debezium.jdbc.JdbcConnection.ResultSetMapper;
import io.debezium.pipeline.source.spi.ChangeTableResultSet;
import io.debezium.relational.Table;
import io.debezium.relational.mapping.CorrectedColumnsResultsSetMapper;

/**
 * The logical representation of a position for the change in the transaction log.
 * During each sourcing cycle it is necessary to query all change tables and then
 * make a total order of changes across all tables.<br>
 * This class represents an open database cursor over the change table that is
 * able to move the cursor forward and report the LSN for the change to which the cursor
 * now points.
 *
 * @author Jiri Pechanec
 *
 */
public class SqlServerChangeTablePointer extends ChangeTableResultSet<SqlServerChangeTable, TxLogPosition> {

    private static final int COL_COMMIT_LSN = 1;
    private static final int COL_ROW_LSN = 2;
    private static final int COL_OPERATION = 3;
    private static final int COL_DATA = 5;

    private ResultSetMapper<Object[]> resultSetMapper;
    private final ResultSet resultSet;
    private final int columnDataOffset;

    public SqlServerChangeTablePointer(SqlServerChangeTable changeTable, ResultSet resultSet) {
        this(changeTable, resultSet, COL_DATA);
    }

    public SqlServerChangeTablePointer(SqlServerChangeTable changeTable, ResultSet resultSet, int columnDataOffset) {
        super(changeTable, resultSet, columnDataOffset);
        // Store references to these because we can't get them from our superclass
        this.resultSet = resultSet;
        this.columnDataOffset = columnDataOffset;
    }

    @Override
    protected int getOperation(ResultSet resultSet) throws SQLException {
        return resultSet.getInt(COL_OPERATION);
    }

    @Override
    protected Object getColumnData(ResultSet resultSet, int columnIndex) throws SQLException {
        if (resultSet.getMetaData().getColumnType(columnIndex) == Types.TIME) {
            return resultSet.getTime(columnIndex);
        }
        return super.getColumnData(resultSet, columnIndex);
    }

    @Override
    protected TxLogPosition getNextChangePosition(ResultSet resultSet) throws SQLException {
        return isCompleted() ? TxLogPosition.NULL
                : TxLogPosition.valueOf(Lsn.valueOf(resultSet.getBytes(COL_COMMIT_LSN)), Lsn.valueOf(resultSet.getBytes(COL_ROW_LSN)));
    }

    /**
     * Check whether TX in currentChangePosition is newer (higher) than TX in previousChangePosition
     * @return true <=> TX in currentChangePosition > TX in previousChangePosition
     * @throws SQLException
     */
    protected boolean isNewTransaction() throws SQLException {
        return (getPreviousChangePosition() != null) &&
                getChangePosition().getCommitLsn().compareTo(getPreviousChangePosition().getCommitLsn()) > 0;
    }

    @Override
    public Object[] getData() throws SQLException {
        if (resultSetMapper == null) {
            this.resultSetMapper = createResultSetMapper(getChangeTable().getSourceTable());
        }
        return resultSetMapper.apply(resultSet);
    }

    /**
     * Internally each row is represented as an array of objects, where the order of values
     * corresponds to the order of columns (fields) in the table schema. However, when capture
     * instance contains only a subset of original's table column, in order to preserve the
     * aforementioned order of values in array, raw database results have to be adjusted
     * accordingly.
     *
     * @param table original table
     * @return a mapper which adjusts order of values in case the capture instance contains only
     * a subset of columns
     */
    private ResultSetMapper<Object[]> createResultSetMapper(Table table) {
        return new CorrectedColumnsResultsSetMapper(table, columnDataOffset);
    }
}
