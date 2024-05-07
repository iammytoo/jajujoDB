package jajujoDB.lib;

public class Column {
    private String name;
    private Class<?> type;

    public Column(String name, Class<?> type) {
        this.name = name;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public Class<?> getType() {
        return type;
    }
}
