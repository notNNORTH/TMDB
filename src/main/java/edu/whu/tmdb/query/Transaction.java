package edu.whu.tmdb.query;



import edu.whu.tmdb.query.operations.impl.*;
import edu.whu.tmdb.query.operations.torch.TorchConnect;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.statement.Statement;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import edu.whu.tmdb.Log.LogManager;
import edu.whu.tmdb.query.operations.Create;
import edu.whu.tmdb.query.operations.CreateDeputyClass;
import edu.whu.tmdb.query.operations.Delete;
import edu.whu.tmdb.query.operations.Drop;
import edu.whu.tmdb.query.operations.Exception.TMDBException;
import edu.whu.tmdb.query.operations.Insert;
import edu.whu.tmdb.query.operations.Select;
import edu.whu.tmdb.query.operations.Update;
import edu.whu.tmdb.query.operations.utils.MemConnect;
import edu.whu.tmdb.query.operations.utils.SelectResult;
import edu.whu.tmdb.level.LevelManager;
import edu.whu.tmdb.memory.MemManager;
import edu.whu.tmdb.memory.Tuple;
import edu.whu.tmdb.memory.TupleList;

public class Transaction {

    public MemManager mem;
    public LevelManager levelManager;
    public LogManager log;

    private MemConnect memConnect;

    public Transaction() throws IOException, JSQLParserException, TMDBException {
//        test21();
        this.mem = new MemManager();
        this.levelManager = mem.levelManager;
        this.memConnect=new MemConnect(mem);
    }


    public void clear() throws IOException {
//        File classtab=new File("/data/data/edu.whu.tmdb/transaction/classtable");
//        classtab.delete();
        File objtab=new File("/data/data/edu.whu.tmdb/transaction/objecttable");
        objtab.delete();
    }

    public void SaveAll( ) throws IOException {
        memConnect.SaveAll();
    }

    public void reload() throws IOException {
        memConnect.reload();
    }

    public void Test(){
        TupleList tpl = new TupleList();
        Tuple t1 = new Tuple();
        t1.tupleHeader = 5;
        t1.tuple = new Object[t1.tupleHeader];
        t1.tuple[0] = "a";
        t1.tuple[1] = 1;
        t1.tuple[2] = "b";
        t1.tuple[3] = 3;
        t1.tuple[4] = "e";
        Tuple t2 = new Tuple();
        t2.tupleHeader = 5;
        t2.tuple = new Object[t2.tupleHeader];
        t2.tuple[0] = "d";
        t2.tuple[1] = 2;
        t2.tuple[2] = "e";
        t2.tuple[3] = 2;
        t2.tuple[4] = "v";

        tpl.addTuple(t1);
        tpl.addTuple(t2);
        String[] attrname = {"attr2","attr1","attr3","attr5","attr4"};
        int[] attrid = {1,0,2,4,3};
        String[]attrtype = {"int","char","char","char","int"};

    }


    public SelectResult query(String k, int op, Statement stmt) {
        //Action action = new Action();
//        action.generate(s);
        ArrayList<Integer> tuples=new ArrayList<>();
        SelectResult selectResult = new SelectResult();
        try {
            //获取生成语法树的类型，用于进一步判断
            String sqlType=stmt.getClass().getSimpleName();

            switch (sqlType) {
                case "CreateTable":
//                    log.WrteLog(s);
                    Create create =new CreateImpl(memConnect);
                    create.create(stmt);
                    break;
                case "CreateDeputyClass":
//                    switch
//                    log.WriteLog(id,k,op,s);
                    CreateDeputyClass createDeputyClass=new CreateDeputyClassImpl(memConnect);
                    createDeputyClass.createDeputyClass(stmt);
                    break;
                case "CreateTJoinDeputyClass":
//                    switch
                    //                   log.WriteLog(id,k,op,s);
                    CreateTJoinDeputyClassImpl createTJoinDeputyClass=new CreateTJoinDeputyClassImpl(memConnect);
                    createTJoinDeputyClass.createTJoinDeputyClass(stmt);
                    break;
                case "Drop":
//                    log.WriteLog(id,k,op,s);
                    Drop drop=new DropImpl(memConnect);
                    drop.drop(stmt);
                    break;
                case "Insert":
//                    log.WriteLog(id,k,op,s);
                    Insert insert=new InsertImpl(memConnect);
                    tuples=insert.insert(stmt);
                    break;
                case "Delete":
 //                   log.WriteLog(id,k,op,s);
                    Delete delete=new DeleteImpl(memConnect);
                    tuples= delete.delete(stmt);
                    break;
                case "Select":
                    Select select=new SelectImpl(memConnect);
                    selectResult=select.select((net.sf.jsqlparser.statement.select.Select) stmt);
                    for (Tuple t:
                         selectResult.getTpl().tuplelist) {
                         tuples.add(t.getTupleId());
                    }
                    break;
                case "Update":
 //                   log.WriteLog(id,k,op,s);
                    Update update=new UpdateImpl(memConnect);
                    tuples=update.update(stmt);
                    break;
                default:
                    break;

            }
        } catch (JSQLParserException e) {
            e.printStackTrace();
        } catch (TMDBException e) {
            e.printStackTrace();
        }
        int[] ints = new int[tuples.size()];
        for (int i = 0; i < tuples.size(); i++) {
            ints[i]=tuples.get(i);
        }
//        action.setKey(ints);
        return selectResult;
    }

    public void test() throws IOException, TMDBException, JSQLParserException {
        TorchConnect torchConnect = new TorchConnect(memConnect,"Torch_Porto_test");
        torchConnect.insert();
        this.SaveAll();
        torchConnect.mapMatching();
    }

    public void test2() {
        TorchConnect torchConnect = new TorchConnect(memConnect,"Torch_Porto_test");
        torchConnect.initEngine();
        torchConnect.test ();
    }

    public void test3() throws IOException, TMDBException, JSQLParserException {
        TorchConnect torchConnect = new TorchConnect(memConnect,"Torch_Porto_test");
        torchConnect.insert();
    }
}

