package edu.whu.tmdb;/*
 * className:${NAME}
 * Package:edu.whu.tmdb
 * Description:
 * @Author: xyl
 * @Create:${DATE} - ${TIME}
 * @Version:v1
 */

import edu.whu.tmdb.query.Transaction;
import edu.whu.tmdb.query.operations.Exception.TMDBException;
import edu.whu.tmdb.query.operations.utils.SelectResult;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;

import java.io.*;

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
            } else {
                SelectResult result = execute(sqlCommand);
                // System.out.println("Result: " + result.toString());
            }
        }


        /*
        long startTime = System.currentTimeMillis();
        long tmp = startTime;

        String fileName = "D:\\cs\\JavaProject\\TMDB\\src\\main\\java\\edu\\whu\\tmdb\\test\\insert_test_1k.txt";
        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            String sqlCommand;
            while ((sqlCommand = br.readLine()) != null) {
                execute(sqlCommand);
                long TimeNow = System.currentTimeMillis();
                System.out.println(sqlCommand);
                System.out.println("spend time: " + (TimeNow - tmp) + "ms, total: " + (TimeNow - startTime) + "ms");
                tmp = TimeNow;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


        long endTime = System.currentTimeMillis();
        System.out.println("spend time: " + (endTime - startTime) + " ms");

         */

        /*
        long startTime=System.nanoTime();
        execute("INSERT INTO company_1k VALUES ('fDXKK', 666, 666);");

        long endTime=System.nanoTime();
        System.out.println("exe执行时间: " + (endTime - startTime) + " ns");*/


        // execute("show tables;");
        // execute("select * from id_vertex;");
//        execute("select * from traj;");
//        execute("select * from trajectory_vertex limit 1;");
//        execute("CREATE CLASS company (name char,age int, salary int);");
//        execute("INSERT INTO company VALUES (aa,20,1000);");
//        execute("create selectdeputy deputy as select * from company limit 1;");
//        execute("select * from traj"+
//                " where traj_name='"+getFileNameWithoutExtension("data/res/raw/porto_raw_trajectory.txt")+"';");
//        execute(args[0]);
//        transaction.test();
//        transaction.test2();
//        insertIntoTrajTable();
        // testMapMatching();
//        testEngine();
//        testTorch3();
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
        Statement stmt = null;
        SelectResult selectResult = new SelectResult();
        try {
            // 使用JSqlparser进行sql语句解析，会根据sql类型生成对应的语法树
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(s.getBytes());
            stmt = CCJSqlParserUtil.parse(byteArrayInputStream);

            selectResult = transaction.query("", -1, stmt);
        }catch (JSQLParserException e) {
            e.printStackTrace();
        }
        if(!stmt.getClass().getSimpleName().toLowerCase().equals("select")){
            transaction.SaveAll();
        }
        return selectResult;
    }
}