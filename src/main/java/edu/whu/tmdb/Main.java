package edu.whu.tmdb;

import edu.whu.tmdb.query.Transaction;
import edu.whu.tmdb.query.operations.Exception.TMDBException;
import edu.whu.tmdb.query.operations.utils.SelectResult;
import edu.whu.tmdb.storage.memory.Tuple;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import static edu.whu.tmdb.util.FileOperation.getFileNameWithoutExtension;

public class Main {
    public static void main(String[] args) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        String sqlCommand;

        // 调试用
        while (true) {
            System.out.print("tmdb> ");
            sqlCommand = reader.readLine().trim();
            if ("exit".equalsIgnoreCase(sqlCommand)) {
                break;
            }else if ("resetdb".equalsIgnoreCase(sqlCommand)) {
                resetDB();
            } else if (!sqlCommand.isEmpty()) {
                SelectResult result = execute(sqlCommand);
                if (result != null) {
                    printResult(result);
                }
            }
        }

        // execute("show tables;");
        // execute(args[0]);
        // transaction.test();
        // transaction.test2();
        // insertIntoTrajTable();
        // testMapMatching();
        // testEngine();
        // testTorch3();
    }

    public static void insertIntoTrajTable(){
        Transaction transaction = Transaction.getInstance();
        transaction.insertIntoTrajTable();
        transaction.SaveAll();
    }

    public static void testEngine() throws IOException {
        Transaction transaction = Transaction.getInstance();
        transaction.testEngine();
    }

    public static void testMapMatching() {
        Transaction transaction = Transaction.getInstance();
        transaction.testMapMatching();
    }

    public static SelectResult execute(String s)  {
        Transaction transaction = Transaction.getInstance();    // 创建一个事务实例
        SelectResult selectResult = null;
        try {
            // 使用JSqlparser进行sql语句解析，会根据sql类型生成对应的语法树
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(s.getBytes());
            Statement stmt = CCJSqlParserUtil.parse(byteArrayInputStream);
            selectResult = transaction.query("", -1, stmt);
            if(!stmt.getClass().getSimpleName().toLowerCase().equals("select")){
                transaction.SaveAll();
            }
        }catch (JSQLParserException e) {
            // e.printStackTrace();    // 打印语法错误的堆栈信息
            System.out.println("syntax error");
        }
        return selectResult;
    }

    private static void printResult(SelectResult result) {
        // 输出表头信息
        StringBuilder tableHeader = new StringBuilder("|");
        for (int i = 0; i < result.getAttrname().length; i++) {
            tableHeader.append(String.format("%-20s", result.getClassName()[i] + "." + result.getAttrname()[i])).append("|");
        }
        System.out.println(tableHeader);

        // 输出元组信息
        for (Tuple tuple : result.getTpl().tuplelist) {
            StringBuilder data = new StringBuilder("|");
            for (int i = 0; i < tuple.tuple.length; i++) {
                data.append(String.format("%-20s", tuple.tuple[i].toString())).append("|");
            }
            System.out.println(data);
        }
    }

    private static void resetDB() {
        // 仓库路径
        String repositoryPath = "D:\\cs\\JavaProject\\TMDB";

        // 子目录路径
        String sysPath = repositoryPath + File.separator + "data\\sys";
        String logPath = repositoryPath + File.separator + "data\\log";
        String levelPath = repositoryPath + File.separator + "data\\level";

        List<String> filePath = new ArrayList<>();
        filePath.add(sysPath);
        filePath.add(logPath);
        filePath.add(levelPath);

        // 遍历删除文件
        for (String path : filePath) {
            File directory = new File(path);

            // 检查目录是否存在
            if (!directory.exists()) {
                System.out.println("目录不存在：" + path);
                return;
            }

            // 获取目录中的所有文件
            File[] files = directory.listFiles();
            if (files == null) { continue; }
            for (File file : files) {
                // 删除文件
                if (file.delete()) {
                    System.out.println("已删除文件：" + file.getAbsolutePath());
                } else {
                    System.out.println("无法删除文件：" + file.getAbsolutePath());
                }
            }
        }
    }


}