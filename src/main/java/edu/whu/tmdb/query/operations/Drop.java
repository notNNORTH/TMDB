package edu.whu.tmdb.query.operations;

import edu.whu.tmdb.query.operations.Exception.TableNotExistError;
import net.sf.jsqlparser.statement.Statement;

import edu.whu.tmdb.query.operations.Exception.TMDBException;

public interface Drop {
    boolean drop(Statement statement) throws TMDBException, TableNotExistError;
}
