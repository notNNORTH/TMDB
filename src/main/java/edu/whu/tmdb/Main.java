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

public class Main {
    public static void main(String[] args) throws TMDBException, JSQLParserException, IOException {
//        execute("CREATE CLASS company (name char,age int, salary int);");
//        execute("select * from traj;");
//        transaction.query("",-1,
//                "create selectdeputy deputy as select name as n, age as a, salary as s from company, test;");
//        execute(args[0]);
//        transaction.test();
//        transaction.test2();
        testTorch();
    }

    public static void testTorch() throws TMDBException, JSQLParserException, IOException {
        Transaction transaction = new Transaction();
        transaction.test3();
        transaction.SaveAll();
    }

    public static void testTorch2() throws TMDBException, JSQLParserException, IOException{
        Transaction transaction = new Transaction();
    }

    public static SelectResult execute(String s) throws TMDBException, JSQLParserException, IOException {
        Transaction transaction = new Transaction();
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