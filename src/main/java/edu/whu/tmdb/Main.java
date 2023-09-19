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

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static edu.whu.tmdb.util.FileOperation.getFileNameWithoutExtension;

public class Main {
    public static void main(String[] args) throws IOException {
//        execute("CREATE CLASS id_vertex (name char,age int, salary int);");
//        execute("select * from id_vertex limit 1;");
//        execute("select * from trajectory_vertex limit 1;");
//        execute("CREATE CLASS company (name char,age int, salary int);");
//        execute("INSERT INTO company VALUES (aa,20,1000);");
//        execute("INSERT INTO company VALUES (ab,30,1000);");
//        execute("INSERT INTO company VALUES (ac,40,1000);");
//        execute("create selectdeputy deputy as select * from company limit 1;");
//        execute("select * from traj"+
//                " where traj_name='"+getFileNameWithoutExtension("data/res/raw/porto_raw_trajectory.txt")+"';");
//        execute(args[0]);
//        transaction.test();
//        transaction.test2();
//        insertIntoTrajTable();
        testMapMatching();
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
        Transaction transaction = Transaction.getInstance();
        Statement stmt = null;
        SelectResult selectResult = new SelectResult();
        try {
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(s.getBytes());
            //使用JSqlparser进行sql语句解析，会根据sql类型生成对应的语法树。
            stmt= CCJSqlParserUtil.parse(byteArrayInputStream);
            selectResult=transaction.query("", -1, stmt);
        }catch (JSQLParserException e) {
            e.printStackTrace();
        }
        if(!stmt.getClass().getSimpleName().toLowerCase().equals("select")){
            transaction.SaveAll();
        }
        return selectResult;
    }
}