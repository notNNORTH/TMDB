package edu.whu.tmdb.Transaction.Transactions;

import edu.whu.tmdb.Transaction.Transactions.Exception.TMDBException;
import edu.whu.tmdb.Transaction.Transactions.utils.SelectResult;

public interface Select {
    SelectResult select(Object stmt) throws TMDBException;
}
