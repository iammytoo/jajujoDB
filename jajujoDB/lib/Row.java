package jajujoDB.lib;

import java.util.List;
import java.util.ArrayList;

public class Row {
    public List<Object> values;

    public Row(int size) {
        values = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            values.add(null);
        }
    }

    public int getValueSize() {
        return values.size();
    }

    public void setValue(int index, Object value) {
        values.set(index, value);
    }

    public Object getValue(int index) {
        return values.get(index);
    }
}
