package edu.whu.tmdb.Transaction.Transactions;

import net.sf.jsqlparser.statement.Statement;

import edu.whu.tmdb.Transaction.Transactions.Exception.TMDBException;

public interface CreateDeputyClass {
    boolean createDeputyClass(Statement stmt) throws TMDBException;
}
