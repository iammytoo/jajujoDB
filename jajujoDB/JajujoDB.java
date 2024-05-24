package jajujoDB;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Scanner;
import java.net.*;
import java.io.*;

import jajujoDB.lib.Row;

public class JajujoDB {
    public static int PORT = 3366; 
    public static void main(String[] args) throws IOException {
        if (args.length == 1) {
            PORT = Integer.parseInt(args[0]);
        }
        ServerSocket s = new ServerSocket(PORT);
        DataBase db = new DataBase();
        Query query = new Query(db);
        Scanner scanner = new Scanner(System.in);
        int tmp = 0;
        try {
            while(true){
            try{
                Socket socket = s.accept();
                
                BufferedReader in = new BufferedReader(
                            new InputStreamReader(
                                    socket.getInputStream())); // データ受信用バッファの設定
                PrintWriter out = new PrintWriter(
                                    new BufferedWriter(
                                    new OutputStreamWriter(
                                            socket.getOutputStream())),true); 
                
                String line = in.readLine();
                Queue<String> commands = new LinkedList<>();
                for(String command :Parser.splitString(line))commands.offer(command);
                String response = "200";
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
                                    System.out.println(fromTableName);
                                    query = query.from(fromTableName);
                                    System.out.println(query);
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
                                String tmp_res = "";
                                for (Row result : results) {
                                    String tmp_result = "";
                                    for(int i =0; i< result.getValueSize();i++)tmp_result+=result.getValue(i)+", ";
                                    tmp_res += tmp_result+"\n";
                                }
                                response = tmp_res;
                                break;
                            }
                            command = commands.poll();
                        }
                    }
                }
                out.println(response);
                socket.close();
                tmp ++;
                System.out.println(tmp);
            }catch(Exception e){
                System.out.println(e);
                continue;
            }
            }
        }finally{
            scanner.close();
            s.close();
        }    
    }
}
