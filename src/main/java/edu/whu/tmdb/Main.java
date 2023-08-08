package edu.whu.tmdb;/*
 * className:${NAME}
 * Package:edu.whu.tmdb
 * Description:
 * @Author: xyl
 * @Create:${DATE} - ${TIME}
 * @Version:v1
 */

import edu.whu.tmdb.Transaction.Transaction;
import edu.whu.tmdb.Transaction.Transactions.Exception.TMDBException;
import net.sf.jsqlparser.JSQLParserException;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws TMDBException, JSQLParserException, IOException {
        Transaction transaction = new Transaction();
//        transaction.query("",-1,"CREATE CLASS company (name char,age int, salary int);");
//        transaction.query("",-1,"INSERT INTO company VALUES (aa,20,3000);");
//        transaction.query("",-1,"INSERT INTO company VALUES (bb,20,4000);");
        transaction.query("",-1,"SELECT name,sum(salary) from company group by name;");
        transaction.SaveAll();
    }
}