package io.debezium.relational.mapping;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A results mapper which adjusts order of values in case the result set contains only a subset of columns.
 * Orders are adjusted such that the new object array has the same length, and only includes values in positions in which the result set had values.
 */
public class CorrectedColumnsResultsSetMapper implements JdbcConnection.ResultSetMapper<Object[]> {

    private Table sourceTable;
    private int columnDataOffset;

    public CorrectedColumnsResultsSetMapper(Table table) {
        this.sourceTable = table;
        this.columnDataOffset = 0;
    }

    public CorrectedColumnsResultsSetMapper(Table table, int columnDataOffset) {
        this.sourceTable = table;
        this.columnDataOffset = columnDataOffset;
    }

    @Override
    public Object[] apply(ResultSet resultSet) throws SQLException {
        final List<String> sourceTableColumns = this.sourceTable.columns()
            .stream()
            .map(Column::name)
            .collect(Collectors.toList());
        final List<String> resultColumns = getResultColumnNames(resultSet);
        final int sourceColumnCount = sourceTableColumns.size();
        final int resultColumnCount = resultColumns.size();

        if (sourceTableColumns.equals(resultColumns)) {
            final Object[] data = new Object[sourceColumnCount];
            for (int i = 0; i < sourceColumnCount; i++) {
                data[i] = getColumnData(resultSet, columnDataOffset + i);
            }
            return data;
        } else {
            final IndicesMapping indicesMapping = new IndicesMapping(sourceTableColumns, resultColumns);
            final Object[] data = new Object[sourceColumnCount];
            for (int i = 0; i < resultColumnCount; i++) {
                int index = indicesMapping.getSourceTableColumnIndex(i);
                data[index] = getColumnData(resultSet, columnDataOffset + i);
            }
            return data;
        }
    }

    private List<String> getResultColumnNames(ResultSet resultSet) throws SQLException {
        final int columnCount = resultSet.getMetaData().getColumnCount() - (columnDataOffset - 1);
        final List<String> columns = new ArrayList<>(columnCount);
        for (int i = 0; i < columnCount; ++i) {
            columns.add(resultSet.getMetaData().getColumnName(columnDataOffset + i));
        }
        return columns;
    }

    protected Object getColumnData(ResultSet resultSet, int columnIndex) throws SQLException {
        // TODO Fix this as it is for sql server only...
        if (resultSet.getMetaData().getColumnType(columnIndex) == Types.TIME) {
            return resultSet.getTime(columnIndex);
        }
        return resultSet.getObject(columnIndex);
    }

    private class IndicesMapping {

        private final Map<Integer, Integer> mapping;

        IndicesMapping(List<String> sourceTableColumns, List<String> captureInstanceColumns) {
            this.mapping = new HashMap<>(sourceTableColumns.size(), 1.0F);

            for (int i = 0; i < captureInstanceColumns.size(); ++i) {
                mapping.put(i, sourceTableColumns.indexOf(captureInstanceColumns.get(i)));
            }

        }

        int getSourceTableColumnIndex(int resultCaptureInstanceColumnIndex) {
            return mapping.get(resultCaptureInstanceColumnIndex);
        }
    }
}
