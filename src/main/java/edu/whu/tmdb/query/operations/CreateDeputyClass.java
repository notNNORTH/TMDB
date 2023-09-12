package edu.whu.tmdb.query.operations;

import net.sf.jsqlparser.statement.Statement;

import edu.whu.tmdb.query.operations.Exception.TMDBException;

public interface CreateDeputyClass {
    boolean createDeputyClass(Statement stmt) throws TMDBException;
}
