package edu.whu.tmdb.query.operations.Exception;

import java.util.Objects;

public class TMDBException extends Exception{

    private String tableName = "";
    private int classId = -1;

    // 使用super方法显示调用父类构造函数
    public TMDBException(String tableName) {
        super(tableName);
        this.tableName = tableName;
    }

    public TMDBException(int classId) {
        super("class with ID: " + classId + " doesn't exist!");
        this.classId = classId;
    }

    public void printError(){
        if (!Objects.equals(tableName, "")) {
            System.out.println(tableName + " doesn't exist!");
        }else if (classId != -1) {
            System.out.println("class with ID: " + classId + " doesn't exist!");
        }
    }

}
