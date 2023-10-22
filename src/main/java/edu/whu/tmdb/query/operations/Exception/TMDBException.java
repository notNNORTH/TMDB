package edu.whu.tmdb.query.operations.Exception;

public class TMDBException extends Exception{

    // 使用super方法显示调用父类构造函数
    public TMDBException(String message) { super(message); }
}
