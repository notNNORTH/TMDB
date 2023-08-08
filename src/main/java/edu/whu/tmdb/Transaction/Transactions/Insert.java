package edu.whu.tmdb.Transaction.Transactions;

import net.sf.jsqlparser.statement.Statement;

import java.util.ArrayList;

import edu.whu.tmdb.Transaction.Transactions.Exception.TMDBException;

public interface Insert {
    ArrayList<Integer> insert(Statement stmt) throws TMDBException;
}
