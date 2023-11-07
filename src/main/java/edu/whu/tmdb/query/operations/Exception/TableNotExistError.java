package edu.whu.tmdb.query.operations.Exception;

public class TableNotExistError extends Exception{
    private String tableName = "";
    private int classId = -1;

    public TableNotExistError(String tableName) {
        super(tableName + " doesn't exist!");
        this.tableName = tableName;
    }

    public TableNotExistError(int classId) {
        super("class with ID: " + classId + " doesn't exist!");
        this.classId = classId;
    }

    public void printError(){
        if (tableName != "") {
            System.out.println(tableName + " doesn't exist!");
        }else if (classId != -1) {
            System.out.println("class with ID: " + classId + " doesn't exist!");
        }
    }
}


