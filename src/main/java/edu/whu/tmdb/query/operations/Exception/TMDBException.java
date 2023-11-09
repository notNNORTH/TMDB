package edu.whu.tmdb.query.operations.Exception;

import java.util.Objects;
import edu.whu.tmdb.query.operations.Exception.ErrorList;

public class TMDBException extends Exception{
    private String name = "";
    private int id = -1;
    private int error = -1;

    // 使用super方法显示调用父类构造函数
    public TMDBException(int error, String name) {
        super(name);
        this.name = name;
        this.error = error;
    }

    public TMDBException(int error, int id) {
        // super("class with ID: " + id + " doesn't exist!");
        this.id = id;
        this.error = error;
    }

    public TMDBException(int error) {
        super("syntax error");
        this.error = error;
    }

    public TMDBException() {}

    public void printError(){
        switch (error) {
            case ErrorList.TABLE_ALREADY_EXISTS:
                System.out.println("table " + name + " already exists"); break;
            case ErrorList.CLASS_NAME_DOES_NOT_EXIST:
                System.out.println("class named " + name + " does not exist"); break;
            case ErrorList.CLASS_ID_DOES_NOT_EXIST:
                System.out.println("class with ID: " + id + " does not exist"); break;
            case ErrorList.COLUMN_NAME_DOES_NOT_EXIST:
                System.out.println("column named " + name + " does not exist"); break;
            case ErrorList.COLUMN_ID_DOES_NOT_EXIST:
                System.out.println("column with ID: " + id + " does not exist"); break;
            case ErrorList.MISSING_FROM_CLAUSE:
                System.out.println("SELECT SYNTAX ERROR: missing FROM-clause entry"); break;
            default:
                System.out.println("ERROR"); break;
        }
    }

}
