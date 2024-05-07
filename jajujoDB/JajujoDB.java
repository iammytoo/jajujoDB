package jajujoDB;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Scanner;

import jajujoDB.lib.Column;
import jajujoDB.lib.Row;

public class JajujoDB {
    public static void main(String[] args) {
        DataBase db = new DataBase();
        Query query = new Query(db);
        Scanner scanner = new Scanner(System.in);
        int tmp = 0;
        while(tmp < 10){
            String line = scanner.nextLine();
            Queue<String> commands = new LinkedList<>();
            for(String command :Parser.splitString(line))commands.offer(command);
            while (!commands.isEmpty()) {
                String command = commands.poll();
                if(command.equals("createtable")){
                    String tableName = commands.poll();
                    int columnCount = Integer.parseInt(commands.poll());
                    String[] columnNames = new String[columnCount];
                    String[] columnTypes = new String[columnCount];
                    for(int i =0;i<columnCount;i++){
                        String[] columnInfo = Parser.bracketSplit(commands.poll());
                        columnNames[i] = columnInfo[0];
                        columnTypes[i] = columnInfo[1];
                    }
                    query.createTable(tableName, columnCount, columnNames, columnTypes);
                }else if(command.equals("upsert")){
                    String tableName = commands.poll();
                    String[] columns = Parser.bracketSplit(commands.poll());
                    String[] values = Parser.bracketSplit(commands.poll());
                    query.upsert(tableName, columns,values);
                }else{
                    while (true) {
                        Boolean breakFlag = false;
                        switch (command) {
                            case "from":
                                String fromTableName = commands.poll();
                                query = query.from(fromTableName);
                                break;
                            case "join":
                                String joinTableName = commands.poll();
                                String[] joinColumns = Parser.bracketSplit(commands.poll());
                                query = query.join(joinTableName, joinColumns[0], joinColumns[1]);
                                break;
                            case "where":
                                String[] params = Parser.bracketSplit(commands.poll());
                                query = query.where(params[0], params[1], params[2]);
                                break;
                            case "select":
                                String[] selectColumns = Parser.bracketSplit(commands.poll());
                                query = query.select(selectColumns);
                                breakFlag = true;
                                break;
                            default:
                                breakFlag = true;
                                break;
                        }
                        if (breakFlag) {
                            List<Row> results = query.execute();
                            for (Row result : results) {
                                System.out.println(result.getValue(0) + " ordered " + result.getValue(1));
                            }
                            break;
                        }
                        command = commands.poll();
                    }
                }
            }
            tmp ++;
            System.out.println(tmp);
        }
        scanner.close();
    }
}
