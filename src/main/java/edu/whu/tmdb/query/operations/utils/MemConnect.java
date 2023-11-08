package edu.whu.tmdb.query.operations.utils;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import au.edu.rmit.bdm.Torch.mapMatching.TorSaver;
import com.alibaba.fastjson2.JSON;
import edu.whu.tmdb.query.operations.Exception.TMDBException;
import edu.whu.tmdb.query.operations.Exception.TableNotExistError;
import edu.whu.tmdb.storage.memory.MemManager;
import edu.whu.tmdb.storage.memory.SystemTable.*;
import edu.whu.tmdb.storage.memory.Tuple;
import edu.whu.tmdb.storage.memory.TupleList;
import edu.whu.tmdb.storage.utils.K;
import edu.whu.tmdb.storage.utils.V;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemConnect {
    // 进行内存操作的一些一些方法和数据
    private static Logger logger = LoggerFactory.getLogger(MemConnect.class);
    private MemManager memManager;
    public static ObjectTable topt;
    private static ClassTable classt;
    private static DeputyTable deputyt;
    private static BiPointerTable biPointerT;
    private static SwitchingTable switchingT;

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    // 1. 私有静态变量，用于保存MemConnect的单一实例
    private static volatile MemConnect instance = null;

    // 2. 私有构造函数，确保不能从类外部实例化
    private MemConnect() {
        // 防止通过反射创建多个实例
        if (instance != null) {
            throw new RuntimeException("Use getInstance() method to get the single instance of this class.");
        }
    }

    // 3. 提供一个全局访问点
    public static MemConnect getInstance(MemManager mem) {
        // 双重检查锁定模式
        if (instance == null) { // 第一次检查
            synchronized (MemConnect.class) {
                if (instance == null) { // 第二次检查
                    instance = new MemConnect(mem);
                }
            }
        }
        return instance;
    }

    private MemConnect(MemManager mem) {
        this.memManager = mem;
        topt = MemManager.objectTable;
        classt = MemManager.classTable;
        deputyt = MemManager.deputyTable;
        biPointerT = MemManager.biPointerTable;
        switchingT = MemManager.switchingTable;
    }

    //获取tuple
    public Tuple GetTuple(int id) {
        rwLock.readLock().lock(); // 获取读锁
        Tuple t = null;
        try {
            Object searchResult = this.memManager.search(new K("t" + id));
            if (searchResult == null)
                t= null;
            if (searchResult instanceof Tuple)
                t = (Tuple) searchResult;
            else if (searchResult instanceof V)
                t= JSON.parseObject(((V) searchResult).valueString, Tuple.class);
            if (t.delete)
                t= null;
        }finally {
            rwLock.readLock().unlock();
            return t;
        }
    }

    //插入tuple
    public void InsertTuple(Tuple tuple) {
        rwLock.writeLock().lock(); // 获取写锁
        try {
            this.memManager.add(tuple);
        }finally {
            rwLock.writeLock().unlock();
        }
    }

    //删除tuple
    public void DeleteTuple(int id) {
        rwLock.writeLock().lock();
        try {
            if (id >= 0) {
                Tuple tuple = new Tuple();
                tuple.tupleId = id;
                tuple.delete = true;
                memManager.add(tuple);
            }
        }finally {
            rwLock.writeLock().unlock();
        }
    }

    // 更新tuple
    public void UpateTuple(Tuple tuple, int tupleId) {
        rwLock.writeLock().unlock();
        try {
            tuple.tupleId = tupleId;
            this.memManager.add(tuple);
        }finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * 给定表名(类名), 获取表在classTable中的id值
     * @param tableName 表名(类名)
     * @return 给定表名(类名)所对应的class id
     * @throws TableNotExistError 不存在给定表名的表，抛出异常
     */
    public int getClassId(String tableName) throws TableNotExistError {
        for (ClassTableItem item : classt.classTableList) {
            if (item.classname.equals(tableName)) {
                return item.classid;
            }
        }
        throw new TableNotExistError(tableName);
    }

    /**
     * 给定表名(类名), 获取表在中属性的数量
     * @param tableName 表名(类名)
     * @return 给定表名(类名)所具有的属性数量(attrNum)
     * @throws TableNotExistError 不存在给定表名的表，抛出异常
     */
    public int getClassAttrnum(String tableName) throws TableNotExistError {
        for (ClassTableItem item : classt.classTableList) {
            if (item.classname.equals(tableName)) {
                return item.attrnum;
            }
        }
        throw new TableNotExistError(tableName);
    }

    /**
     * 给定表id(类id), 获取表在classTable中的属性数量
     * @param classId 表名(类名)
     * @return 给定表名(类名)所具有的属性数量(attrNum)
     * @throws TableNotExistError 不存在给定表名的表，抛出异常
     */
    public int getClassAttrnum(int classId) throws TableNotExistError {
        for (ClassTableItem item : classt.classTableList) {
            if(item.classid == classId){
                return item.attrnum;
            }
        }
        throw new TableNotExistError(classId);
    }

    /**
     * 用于获取插入位置对应的属性id列表 (attrid)
     * @param classId insert对应的表id/类id
     * @param columns insert对应的属性名称列表
     * @return 属性名列表对应的attrid列表
     */
    public int[] getAttridList(int classId, List<String> columns) throws TableNotExistError {
        int attrnum = getClassAttrnum(classId);
        int[] attridList = new int[attrnum];

        // 遍历当前的ClassTableItem
        for (ClassTableItem classTableItem : getClasst().classTableList) {
            if (classTableItem.classid != classId) {
                continue;
            }
            // 找到对应classId的类，遍历当前属性名称列表columns
            for (int i = 0; i < attrnum; i++) {
                if (!columns.get(i).equals(classTableItem.attrname)) {
                    continue;
                }
                // 若colname和当前item对应的attrname相同，获取attrid放到对应位置
                attridList[i] = classTableItem.attrid;
            }
        }

        return attridList;
    }

    /**
     * 给定表名，获取表下的所有元组
     * @param fromItem 表名
     * @return 查询语句中，该表之下所具有的所有元组
     * @throws TableNotExistError 不存在给定表名的表，抛出异常
     */
    public TupleList getTupleList(FromItem fromItem) throws TableNotExistError {
        int classId = getClassId(((Table) fromItem).getName());
        TupleList tupleList = new TupleList();
        for (ObjectTableItem item : getTopt().objectTableList) {
            if (item.classid != classId) {
                continue;
            }
            Tuple tuple = GetTuple(item.tupleid);
            if (tuple != null && !tuple.delete) {
                tuple.setTupleId(item.tupleid);
                tupleList.addTuple(tuple);
            }
        }
        return tupleList;
    }

    /**
     * 给定表名，获取表名class table的副本
     * @param fromItem 表名
     * @return 表名对应的class table副本
     * @throws TableNotExistError 不存在给定表名的表，抛出异常
     */
    public ArrayList<ClassTableItem> copyClassTableList(FromItem fromItem) throws TableNotExistError{
        ArrayList<ClassTableItem> classTableList = new ArrayList<>();
        for (ClassTableItem item : getClasst().classTableList){
            if (item.classname.equals(((Table)fromItem).getName())){
                // 硬拷贝，不然后续操作会影响原始信息
                ClassTableItem classTableItem = item.getCopy();
                if (fromItem.getAlias() != null) {
                    classTableItem.alias = fromItem.getAlias().getName();
                }
                classTableList.add(classTableItem);
            }
        }
        if (classTableList.isEmpty()) {
            throw new TableNotExistError(((Table)fromItem).getName());
        }
        return classTableList;
    }


    public boolean Condition(String attrtype, Tuple tuple, int attrid, String value1) {
        String value = value1.replace("\"", "");
        switch (attrtype) {
            case "int":
                int value_int = Integer.parseInt(value);
                if (Integer.parseInt((String) tuple.tuple[attrid]) == value_int)
                    return true;
                break;
            case "char":
                String value_string = value;
                if (tuple.tuple[attrid].equals(value_string))
                    return true;
                break;

        }
        return false;
    }

    public void SaveAll() {
        memManager.saveAll();
    }

    public void reload() {
        try {
            memManager.loadClassTable();
            memManager.loadDeputyTable();
            memManager.loadBiPointerTable();
            memManager.loadSwitchingTable();
        }catch (IOException e){
            logger.error(e.getMessage());
        }
    }

    public static class OandB {
        public List<ObjectTableItem> o = new ArrayList<>();
        public List<BiPointerTableItem> b = new ArrayList<>();

        public OandB() {
        }

        public OandB(MemConnect.OandB oandB) {
            this.o = oandB.o;
            this.b = oandB.b;
        }

        public OandB(List<ObjectTableItem> o, List<BiPointerTableItem> b) {
            this.o = o;
            this.b = b;
        }
    }


    public static ObjectTable getTopt() {
        return topt;
    }

    public static ClassTable getClasst() {
        return classt;
    }

    public static DeputyTable getDeputyt() {
        return deputyt;
    }

    public static BiPointerTable getBiPointerT() {
        return biPointerT;
    }

    public static SwitchingTable getSwitchingT() {
        return switchingT;
    }
}
