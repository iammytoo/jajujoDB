package jajujoDB.lib;

import java.util.ArrayList;
import java.util.List;

public class Table {
    public String name;
    private List<Column> columns;
    private List<Row> rows;

    public Table(String name) {
        this.name = name;
        this.columns = new ArrayList<>();
        this.rows = new ArrayList<>();
    }

    public void addColumn(Column column) {
        columns.add(column);
    }

    public int getColumnIndex(String columnName) {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).getName().equals(columnName)) {
                return i;
            }
        }
        throw new IllegalArgumentException("Column not found: " + columnName);
    }

    public void addRow(Row row) {
        if (row.values.size() != columns.size()) {
            throw new IllegalArgumentException("Row does not match table columns.");
        }
        rows.add(row);
    }

    public List<Row> getRows() {
        return rows;
    }

    public List<Column> getColumns() {
        return columns;
    }
}
