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
    public static void main(String[] args) throws TMDBException, JSQLParserException, IOException {

        Transaction transaction = Transaction.getInstance();
        execute("CREATE CLASS company (name char,age int, salary int);");
        execute("insert into company values (a,1,1)");
        transaction.SaveAll();
        execute("select * from company");
        execute("insert into company values (a,1,1)");
        transaction.SaveAll();
        execute("insert into company values (a,1,1)");
        transaction.SaveAll();
        execute("insert into company values (a,1,1)");
        transaction.SaveAll();
        execute("insert into company values (a,1,1)");
        transaction.SaveAll();
        execute("insert into company values (a,1,1)");
        transaction.SaveAll();
        execute("insert into company values (a,1,1)");
        transaction.SaveAll();
        return;
//        execute("select * from traj;");
//        execute("select * from traj"+
//                " where traj_name='"+getFileNameWithoutExtension("data/res/raw/porto_raw_trajectory.txt")+"';");
//        execute(args[0]);
//        transaction.test();
//        transaction.test2();
//        testTorch();
//        testTorch2();
    }

    public static void testTorch() throws TMDBException, JSQLParserException, IOException {
        Transaction transaction = Transaction.getInstance();
        //transaction.test3();
        transaction.SaveAll();
    }

    public static void testTorch2() throws TMDBException, JSQLParserException, IOException{
        Transaction transaction = Transaction.getInstance();
        //transaction.test();
    }

    public static SelectResult execute(String s) throws TMDBException, JSQLParserException, IOException {
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