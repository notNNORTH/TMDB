package edu.whu.tmdb.Transaction.Transactions;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.statement.Statement;

import java.util.ArrayList;

import edu.whu.tmdb.Transaction.Transactions.Exception.TMDBException;

public interface Delete {
    ArrayList<Integer> delete(Statement statement) throws JSQLParserException, TMDBException;
}
