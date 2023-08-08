package edu.whu.tmdb.Transaction.Transactions;

import net.sf.jsqlparser.statement.Statement;

import edu.whu.tmdb.Transaction.Transactions.Exception.TMDBException;

public interface Drop {
    boolean drop(Statement statement) throws TMDBException;
}
