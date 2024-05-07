package jajujoDB;

import java.util.List;

import jajujoDB.lib.Column;
import jajujoDB.lib.Row;

public class JajujoDB {
    public static void main(String[] args) {
        DataBase db = new DataBase();
        Query query = new Query(db);

        // テーブルの作成とデータの挿入
        query.createTable("users",
                          new Column("id", Integer.class),
                          new Column("name", String.class),
                          new Column("age", Integer.class));

        query.createTable("orders",
                          new Column("order_id", Integer.class),
                          new Column("user_id", Integer.class),
                          new Column("product", String.class));

        Row user1 = new Row(3);
        user1.setValue(0, 1);
        user1.setValue(1, "Alice");
        user1.setValue(2, 24);
        query.upsert("users", user1);

        Row user2 = new Row(3);
        user2.setValue(0, 2);
        user2.setValue(1, "Bob");
        user2.setValue(2, 27);
        query.upsert("users", user2);

        Row order1 = new Row(3);
        order1.setValue(0, 101);
        order1.setValue(1, 1);
        order1.setValue(2, "Laptop");
        query.upsert("orders", order1);

        Row order2 = new Row(3);
        order2.setValue(0, 102);
        order2.setValue(1, 2);
        order2.setValue(2, "Smartphone");
        query.upsert("orders", order2);

        // Join users and orders where user age is 25 or older
        List<Row> results = query.from("users")
                                 .join("orders", "user_id", "users", "id")
                                 .where(row -> (Integer) row.getValue(2) >= 25) // Assumes age is at index 2
                                 .select("name", "product")
                                 .execute();

        for (Row result : results) {
            System.out.println(result.getValue(0) + " ordered " + result.getValue(1));
        }
    }
}
