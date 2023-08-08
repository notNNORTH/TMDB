package edu.whu.tmdb.Transaction;

//import static edu.whu.tmdb.Level.Test.*;


import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import edu.whu.tmdb.Log.LogManager;
import edu.whu.tmdb.Transaction.Transactions.Create;
import edu.whu.tmdb.Transaction.Transactions.CreateDeputyClass;
import edu.whu.tmdb.Transaction.Transactions.Delete;
import edu.whu.tmdb.Transaction.Transactions.Drop;
import edu.whu.tmdb.Transaction.Transactions.Exception.TMDBException;
import edu.whu.tmdb.Transaction.Transactions.Insert;
import edu.whu.tmdb.Transaction.Transactions.Select;
import edu.whu.tmdb.Transaction.Transactions.Update;
import edu.whu.tmdb.Transaction.Transactions.impl.CreateDeputyClassImpl;
import edu.whu.tmdb.Transaction.Transactions.impl.CreateImpl;
import edu.whu.tmdb.Transaction.Transactions.impl.DeleteImpl;
import edu.whu.tmdb.Transaction.Transactions.impl.DropImpl;
import edu.whu.tmdb.Transaction.Transactions.impl.InsertImpl;
import edu.whu.tmdb.Transaction.Transactions.impl.SelectImpl;
import edu.whu.tmdb.Transaction.Transactions.impl.UpdateImpl;
import edu.whu.tmdb.Transaction.Transactions.utils.MemConnect;
import edu.whu.tmdb.Transaction.Transactions.utils.SelectResult;
import edu.whu.tmdb.level.LevelManager;
import edu.whu.tmdb.memory.MemManager;
import edu.whu.tmdb.memory.Tuple;
import edu.whu.tmdb.memory.TupleList;

public class Transaction {

    public MemManager mem;
    public LevelManager levelManager;
    public LogManager log;

    private MemConnect memConnect;

//    public TransAction() throws IOException {}

    public Transaction() throws IOException, JSQLParserException, TMDBException {
//        test21();
        this.mem = new MemManager();
        this.levelManager = mem.levelManager;
        this.memConnect=new MemConnect(mem);
//        TorchSQLiteHelper torchSQLiteHelper = new TorchSQLiteHelper(context, "/data/data/edu.whu.tmdb/res/Torch_Porto_test/Torch/db/Torch_Porto_test.db");
//        torchSQLiteHelper.execSQL("select 1");
//        new TrajectoryUtils(memConnect);

//        DeleteImpl delete=new DeleteImpl(memConnect);
//        String sql="delete from traj where traj_id=2";
//        delete.delete(CCJSqlParserUtil.parse(sql));
//        TorchConnect.init(memConnect, "Torch_Porto_test",context);
//
////        torchConnect.mapMatching();
////        torchConnect.insert();
////        TorchConnect.getTorchConnect().initEngine();
//        TorchConnect.getTorchConnect().testSQLiteHelper();
//        TorchConnect.test();
//        classt = mem.classTable;
//        deputyt = mem.deputyTable;
//        biPointerT = mem.biPointerTable;
//        switchingT = mem.switchingTable;
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


    public String query(String k, int op, String s) {

//        memConnect.reload();
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(s.getBytes());
        //Action action = new Action();
//        action.generate(s);
        ArrayList<Integer> tuples=new ArrayList<>();
        try {
            //使用JSqlparser进行sql语句解析，会根据sql类型生成对应的语法树。
            Statement stmt= CCJSqlParserUtil.parse(byteArrayInputStream);
//            String[] aa = new String[2];
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
 //                   log.WriteLog(id,k,op,s);
                    CreateDeputyClass createDeputyClass=new CreateDeputyClassImpl(memConnect);
                    createDeputyClass.createDeputyClass(stmt);
                    break;
                //TODO TMDB
                //加入针对CreateTJoinDeputyClass这种statement的处理逻辑

//                case "Create":
//                    log.WriteLog(s);
//                    CreateUnionDeputy(aa);
//                    new AlertDialog.Builder(context).setTitle("提示").setMessage("创建Union代理类成功").setPositiveButton("确定",null).show();
//                    break;
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
                    SelectResult selectResult=select.select((net.sf.jsqlparser.statement.select.Select) stmt);
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
        return s;
    }

//    public void test() throws IOException, TMDBException, JSQLParserException {
////        TorchSQLiteHelper torchSQLiteHelper = new TorchSQLiteHelper(context, "/data/data/edu.whu.tmdb/res/Torch_Porto_test/Torch/db/Torch_Porto_test.db");
////        torchSQLiteHelper.execSQL("select 1");
//
//
////        torchConnect.mapMatching();
////        torchConnect.insert();
////        TorchConnect.getTorchConnect().initEngine();
//        TorchConnect torchConnect = new TorchConnect(memConnect,"/data/data/edu.whu.tmdb/res/Torch_Porto_test");
//        torchConnect.mapMatching();
////        torchConnect.updateMeta();
////        torchConnect.initEngine();
////        torchConnect.testSQLiteHelper();
////        torchConnect.test();
//    }
//
//    public void initTorchEngine(Context context) {
//        TorchConnect.init(memConnect, "Torch_Porto_test",context);
//        TorchConnect.getTorchConnect().initEngine();
//    }
}
