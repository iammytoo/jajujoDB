package jajujoDB;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

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

    public Query createTable(String tableName, int columnCount, String[] columnNames, String[] columnTypes) {
        Column[] columns = new Column[columnCount];
        for (int i = 0; i < columnCount; i++) {
            columns[i] = new Column(columnNames[i], parseClass(columnTypes[i]));
        }
        Table newTable = new Table(tableName);
        for (Column column : columns) {
            newTable.addColumn(column);
        }
        db.addTable(newTable);
        return this;
    }

    private Class<?> parseClass(String type) {
        switch (type.toLowerCase()) {
            case "int":
                return Integer.class;
            case "string":
                return String.class;
            case "datetime":
                return LocalDateTime.class;
            default:
                throw new IllegalArgumentException("Unsupported type: " + type);
        }
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

    public Query join(String tableName, String joinColumn, String onColumn) {
        Table joinTable = db.getTable(tableName);
        Table onTableRef = this.currentTable;
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
        Table tempTable = new Table("temporary_table_for_join_"+tableName+"_"+onTableRef.name);
        for (Column column : onTableRef.getColumns()) {
            tempTable.addColumn(column);
        }
        for (Column column : joinTable.getColumns()) {
            tempTable.addColumn(column);
        }
        this.currentTable = tempTable;
        return this;
    }

    public Query upsert(String tableName, String[] columns, String[] values) {
        Table table = db.getTable(tableName);
        Row row = new Row(table.getColumnsSize());
        for(int i = 0;i<columns.length;i++){
            int index = table.getColumnIndex(columns[i]);
            if(table.getColumnType(columns[i]).equals(Integer.class)){
                row.setValue(index, Integer.parseInt(values[i]));
            } else if(table.getColumnType(columns[i]).equals(LocalDateTime.class)){
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
                LocalDateTime dateTime = LocalDateTime.parse(values[i], formatter);
                row.setValue(index, dateTime);
            } else {
                row.setValue(index, values[i]);
            }
        }
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

    public Query where(String column, String operator, String value) {
        Predicate<Row> condition = (row -> true);
        int columnIndex = currentTable.getColumnIndex(column);
        if(currentTable.getColumnType(column).equals(Integer.class)){
            switch (operator) {
                case "==":
                    condition = (row -> (Integer) row.getValue(columnIndex) == Integer.parseInt(value));
                    break;
                case ">":
                    condition = (row -> (Integer) row.getValue(columnIndex) > Integer.parseInt(value));
                    break;
                case "<":
                    condition = (row -> (Integer) row.getValue(columnIndex) < Integer.parseInt(value));
                    break;
                case ">=":
                    condition = (row -> (Integer) row.getValue(columnIndex) >= Integer.parseInt(value));
                    break;
                case "<=":
                    condition = (row -> (Integer) row.getValue(columnIndex) <= Integer.parseInt(value));
                    break;
                default:
                    break;
            }
        } else if(currentTable.getColumnType(column).equals(LocalDateTime.class)){
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
            switch (operator) {
                case "==":
                    condition = (row -> {
                        LocalDateTime rowDateTime = (LocalDateTime) row.getValue(columnIndex);
                        return rowDateTime.isEqual(LocalDateTime.parse(value,formatter));
                });
                    break;
                case ">":
                    condition = (row -> {
                        LocalDateTime rowDateTime = (LocalDateTime) row.getValue(columnIndex);
                        return rowDateTime.isAfter(LocalDateTime.parse(value,formatter));
                });
                    break;
                case "<":
                    condition = (row -> {
                        LocalDateTime rowDateTime = (LocalDateTime) row.getValue(columnIndex);
                        return rowDateTime.isBefore(LocalDateTime.parse(value,formatter));
                });
                    break;
                case ">=":
                    condition = (row -> {
                        LocalDateTime rowDateTime = (LocalDateTime) row.getValue(columnIndex);
                        return rowDateTime.isAfter(LocalDateTime.parse(value,formatter)) || rowDateTime.isEqual(LocalDateTime.parse(value,formatter));
                    });
                    break;
                case "<=":
                    condition = (row -> {
                        LocalDateTime rowDateTime = (LocalDateTime) row.getValue(columnIndex);
                        return rowDateTime.isBefore(LocalDateTime.parse(value,formatter)) || rowDateTime.isEqual(LocalDateTime.parse(value,formatter));
                    });
                    break;
                default:
                    break;
            }
        } else {
            condition = (row -> row.getValue(columnIndex).equals(value));
        }
        this.rows = this.rows.stream().filter(condition).collect(Collectors.toList());
        return this;
    }

    public List<Row> execute() {
        return this.rows;
    }
}