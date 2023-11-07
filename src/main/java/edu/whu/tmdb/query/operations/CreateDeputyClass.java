package edu.whu.tmdb.query.operations;

import edu.whu.tmdb.query.operations.Exception.TableNotExistError;
import net.sf.jsqlparser.statement.Statement;

import edu.whu.tmdb.query.operations.Exception.TMDBException;

import java.io.IOException;

public interface CreateDeputyClass {
    boolean createDeputyClass(Statement stmt) throws TMDBException, IOException, TableNotExistError;
}
