package jajujoDB;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import jajujoDB.lib.*;

public class Query {
    private DataBase db;
    private List<Row> rows;
    private Table currentTable;

    public Query(DataBase db) {
        this.db = db;
        this.rows = new ArrayList<>();
    }

    public Query from(String tableName) {
        this.currentTable = db.getTable(tableName);
        this.rows = new ArrayList<>(currentTable.getRows());
        return this;
    }

    public Query createTable(String tableName, Column... columns) {
        Table newTable = new Table(tableName);
        for (Column column : columns) {
            newTable.addColumn(column);
        }
        db.addTable(newTable);
        return this;
    }

    public Query select(String... columnNames) {
        List<Integer> indices = new ArrayList<>();
        for (String columnName : columnNames) {
            indices.add(currentTable.getColumnIndex(columnName));
        }

        List<Row> selectedRows = new ArrayList<>();
        for (Row row : this.rows) {
            Row newRow = new Row(indices.size());
            int newIndex = 0;
            for (int index : indices) {
                newRow.setValue(newIndex++, row.getValue(index));
            }
            selectedRows.add(newRow);
        }
        this.rows = selectedRows;
        return this;
    }

    public Query join(String tableName, String joinColumn, String onTable, String onColumn) {
        Table joinTable = db.getTable(tableName);
        Table onTableRef = db.getTable(onTable);
        int joinColumnIndex = joinTable.getColumnIndex(joinColumn);
        int onColumnIndex = onTableRef.getColumnIndex(onColumn);
        List<Row> newRows = new ArrayList<>();

        for (Row row : this.rows) {
            for (Row joinRow : joinTable.getRows()) {
                if (row.getValue(onColumnIndex).equals(joinRow.getValue(joinColumnIndex))) {
                    Row newRow = new Row(currentTable.getColumns().size() + joinTable.getColumns().size());
                    int i = 0;
                    for (Object value : row.values) {
                        newRow.setValue(i++, value);
                    }
                    for (Object value : joinRow.values) {
                        newRow.setValue(i++, value);
                    }
                    newRows.add(newRow);
                }
            }
        }
        this.rows = newRows;
        Table tempTable = new Table("temporary_table_for_join_"+tableName+"_"+onTable);
        for (Column column : onTableRef.getColumns()) {
            tempTable.addColumn(column);
        }
        for (Column column : joinTable.getColumns()) {
            tempTable.addColumn(column);
        }
        this.currentTable = tempTable;
        return this;
    }

    public Query upsert(String tableName, Row row) {
        Table table = db.getTable(tableName);
        Object keyValue = row.getValue(0);
        boolean found = false;
        for (Row existingRow : table.getRows()) {
            if (existingRow.getValue(0).equals(keyValue)) {
                for (int i = 0; i < table.getColumns().size(); i++) {
                    existingRow.setValue(i, row.getValue(i));
                }
                found = true;
                break;
            }
        }
        if (!found) {
            table.addRow(row);
        }
        return this;
    }

    public Query where(Predicate<Row> condition) {
        this.rows = this.rows.stream().filter(condition).collect(Collectors.toList());
        return this;
    }

    public List<Row> execute() {
        return this.rows;
    }
}