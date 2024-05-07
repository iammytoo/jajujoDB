package jajujoDB;

import java.util.HashMap;
import java.util.Map;

import jajujoDB.lib.Table;

public class DataBase {
    private Map<String, Table> tables = new HashMap<>();

    public void addTable(Table table) {
        tables.put(table.name, table);
    }

    public Table getTable(String name) {
        return tables.get(name);
    }
}
