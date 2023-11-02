/**
 * @className: InsertImpl
 * @Package: edu.whu.tmdb.query.operations.impl
 * @Description: select的输出结果，对应传统数据库打印的结果，另外此处附带其他属性若干
 * Last modified by lzp, 2023.10.26
 */

package edu.whu.tmdb.query.operations.impl;

import au.edu.rmit.bdm.Torch.base.model.Coordinate;
import au.edu.rmit.bdm.Torch.base.model.TrajEntry;
import edu.whu.tmdb.storage.memory.MemManager;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import edu.whu.tmdb.query.operations.Exception.TMDBException;
import edu.whu.tmdb.query.operations.Insert;
import edu.whu.tmdb.query.operations.utils.MemConnect;
import edu.whu.tmdb.query.operations.utils.SelectResult;
import edu.whu.tmdb.query.operations.utils.traj.TrajTrans;
import edu.whu.tmdb.storage.memory.SystemTable.BiPointerTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.ClassTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.DeputyTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.ObjectTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.SwitchingTableItem;
import edu.whu.tmdb.storage.memory.Tuple;
import edu.whu.tmdb.storage.memory.TupleList;

public class InsertImpl implements Insert {
    private MemConnect memConnect;

    ArrayList<Integer> indexs = new ArrayList<>();

    public InsertImpl() {
        this.memConnect = MemConnect.getInstance(MemManager.getInstance());
    }

    @Override
    public ArrayList<Integer> insert(Statement stmt) throws TMDBException, IOException {
        net.sf.jsqlparser.statement.insert.Insert insertStmt = (net.sf.jsqlparser.statement.insert.Insert) stmt;
        Table table = insertStmt.getTable();        // 解析insert对应的表
        List<String> attrNames = new ArrayList<>(); // 解析插入的字段名
        if (insertStmt.getColumns() == null){
            attrNames = getColumns(table.getName());
        }
        else{
            int insertColSize = insertStmt.getColumns().size();
            for (int i = 0; i < insertColSize; i++) {
                attrNames.add(insertStmt.getColumns().get(i).getColumnName());
            }
        }

        // 对应含有子查询的插入语句
        SelectImpl select = new SelectImpl();
        SelectResult selectResult = select.select(insertStmt.getSelect());

        // tuplelist存储需要插入的tuple部分
        TupleList tupleList = selectResult.getTpl();
        execute(table.getName(), attrNames, tupleList);
        return indexs;
    }

    /**
     *
     * @param tableName 表名/类名
     * @param columns 表/类所具有的属性列表
     * @param tupleList 要插入的元组列表
     * @throws TMDBException
     * @throws IOException
     */
    public void execute(String tableName, List<String> columns, TupleList tupleList) throws TMDBException, IOException {
        int classId = memConnect.getClassId(tableName);         // 类id
        int attrNum = memConnect.getClassAttrnum(tableName);    // 属性的数量
        int[] attrIdList = memConnect.getAttridList(classId, columns);         // 插入的属性对应的attrid列表
        for (Tuple tuple : tupleList.tuplelist) {
            if (tuple.tuple.length != columns.size()){
                throw new TMDBException("Insert error: columns size doesn't match tuple size");
            }
            indexs.add(insert(classId, columns, tuple, attrNum, attrIdList));
        }
    }

    /**
     *
     * @param classId 表/类id
     * @param columns 表/类所具有的属性列表
     * @param tupleList 要插入的元组列表
     * @throws TMDBException
     * @throws IOException
     */
    public void execute(int classId, List<String> columns, TupleList tupleList) throws TMDBException, IOException {
        int attrNum = memConnect.getClassAttrnum(classId);
        int[] attrIdList = memConnect.getAttridList(classId, columns);
        for (Tuple tuple : tupleList.tuplelist) {
            if (tuple.tuple.length != columns.size()){
                throw new TMDBException("Insert error: columns size doesn't match tuple size");
            }
            indexs.add(insert(classId, columns, tuple, attrNum, attrIdList));
        }
    }

    /**
     *
     * @param classId 要插入的类id
     * @param columns 代理类的属性名列表
     * @param tuple 要insert的元组tuple
     * @return
     * @throws TMDBException
     * @throws IOException
     */
    public int executeTuple(int classId, List<String> columns, Tuple tuple) throws TMDBException, IOException {
        int attrNum = memConnect.getClassAttrnum(classId);
        int[] attridList = memConnect.getAttridList(classId, columns);

        if (tuple.tuple.length != columns.size()){
            throw new TMDBException("Insert error: columns size doesn't match tuple size");
        }
        int insert = insert(classId, columns, tuple, attrNum, attridList);
        indexs.add(insert);
        return insert;
    }


    /**
     * 分为几个阶段
     * 第一阶段插入tuple
     * @param classId 插入表/类对应的id
     * @param columns 表/类所具有的属性名列表（来自insert语句）
     * @param tuple 要插入的元组
     * @param attrNum 元组包含的属性数量（系统表中获取）
     * @param attrId 插入属性对应的attrId列表（根据insert的属性名，系统表中获取）
     * @return
     * @throws TMDBException
     */
    private Integer insert(int classId, List<String> columns, Tuple tuple, int attrNum, int[] attrId) throws TMDBException, IOException {
        // 1.直接在对应类中插入tuple
        // 1.1 获取新插入元组的id
        int tupleid = MemConnect.getTopt().maxTupleId++;

        // 1.2 将tuple转换为可插入的形式
        Object[] temp = new Object[attrNum];
        for (int i = 0; i < attrId.length; i++) {
            temp[attrId[i]] = tuple.tuple[i];
        }
        tuple.setTuple(tuple.tuple.length, tupleid, classId, temp);

        // 1.3 元组插入操作
        memConnect.InsertTuple(tuple);
        MemConnect.getTopt().objectTableList.add(new ObjectTableItem(classId, tupleid));

        // 2.找到所有的代理类，进行递归插入
        // 2.1 找到源类所有的代理类
        ArrayList<Integer> DeputyIdList = getDeputyByOriginId(classId);

        // 2.2 将元组转换为代理类应有的形式
        if (!DeputyIdList.isEmpty()) {

            for (int deputyCalssId : DeputyIdList) {

                HashMap<String, String> attrNameHashMap = getAttrNameHashMap(classId, deputyCalssId, columns);       // 这他妈有什么用呢？既然需要为什么刚刚不一起拿出来算了
                List<String> tempColumns = getInsertColumns(attrNameHashMap, columns);    // 根据源类属性名列表获取代理类属性名列表
                Tuple tuple1 = getDeputyTuple(attrNum, tuple, columns);     // 将插入源类的元组tuple转换为插入代理类的元组tuple1

                // 2.3 递归插入
                int i1 = executeTuple(deputyCalssId, tempColumns, tuple1);
                MemConnect.getBiPointerT().biPointerTableList.add(new BiPointerTableItem(classId, tupleid, deputyCalssId, i1));
            }
        }

        return tupleid;
    }


    /**
     * 将属性id与属性id的对应关系，转换成是属性名到属性名的对应关系。
     * @param map 源类到一个代理类之间的属性id哈希映射
     * @param classId 源类的id
     * @param tempClassId 代理类id
     * @return
     */
    private HashMap<String, String> trans(HashMap<Integer, Integer> map, int classId, int tempClassId) {
        HashMap<String, String> map2 = new HashMap<>();
        HashMap<Integer, String> tempmap1 = new HashMap<>();
        HashMap<Integer, String> tempmap2 = new HashMap<>();
        for (int i = 0; i < MemConnect.getClasst().classTableList.size(); i++) {
            ClassTableItem classTableItem = MemConnect.getClasst().classTableList.get(i);
            if(classTableItem.classid == classId){
                tempmap1.put(classTableItem.attrid, classTableItem.attrname);
            }
            if(classTableItem.classid == tempClassId){
                tempmap2.put(classTableItem.attrid, classTableItem.attrname);
            }
        }
        for (int i : map.keySet()){
            map2.put(tempmap1.get(i), tempmap2.get(map.get(i)));
        }
        return map2;
    }

    /**
     * 给定源类属性名列表，获取其代理类对应属性名列表
     * @param map
     * @param columns
     * @return
     */
    private List<String> getInsertColumns(HashMap<String, String> map, List<String> columns) {
        List<String> res = new ArrayList<>();
        for (String column : columns) {
            if (map.containsKey(column)){
                res.add(map.get(column));
            }
        }
        return res;
    }

    /**
     *
     * @param attrNum 源类属性名->代理类属性名的哈希表
     * @param tuple 插入源类中的tuple
     * @param columns 源类属性名列表
     * @return
     */
    private Tuple getDeputyTuple(int attrNum, Tuple tuple, List<String> columns) {
        Tuple res = new Tuple();
        Object[] temp = new Object[attrNum];
        int i = 0;
        for(String s : columns){
            temp[i] = tuple.tuple[columns.indexOf(s)];
            i++;
        }
        res.tuple = temp;
        return res;
    }


    // 给定表名，返回该表的属性列表，注：未加异常处理
    public List<String> getColumns(String tableName){
        List<String> colName = new ArrayList<>();
        for (ClassTableItem classTableItem : MemConnect.getClasst().classTableList) {
            if(classTableItem.classname.equals(tableName)){
                colName.add(classTableItem.attrname);
            }
        }
        return colName;
    }

    /**
     * 得到源类和每个对应代理类的属性id的Hashmap（此处遍历一遍switchingTable，可以考虑使用更高效的算法）
     * @param deputySize 源类对应代理类的数量
     * @param oriId 源类的id
     * @return 存储哈希映射的列表，每个哈希映射对应的是源类和一个代理类之间属性列表的映射关系
     */
    private ArrayList<HashMap<Integer,Integer>> getDeputyAttr(int deputySize, int oriId) {
        int i = 0;
        int c = -1;
        ArrayList<HashMap<Integer, Integer>> res = new ArrayList<>();
        while (i < MemConnect.getSwitchingT().switchingTableList.size()) {     // 遍历所有交换表项
            SwitchingTableItem switchingTableItem = MemConnect.getSwitchingT().switchingTableList.get(i);
            if (switchingTableItem.oriId == oriId){
                c = switchingTableItem.deputyId;      // 获取源类id对应的代理类id
                HashMap<Integer, Integer> map = new HashMap<>();        // map中存储源类属性id到该代理类属性id之间的所有映射
                while (i < MemConnect.getSwitchingT().switchingTableList.size() &&
                        MemConnect.getSwitchingT().switchingTableList.get(i).deputyId == c){
                    map.put(MemConnect.getSwitchingT().switchingTableList.get(i).oriAttrid,
                            MemConnect.getSwitchingT().switchingTableList.get(i).deputyAttrId);
                    i++;
                }
                res.add(map);
            }
            else{
                i++;
            }
        }
        return res;
    }

    /**
     * 获取源类属性列表->代理类属性列表的哈希映射列表（注：可能有的源类属性不在代理类中）
     * @param originClassId 源类的class id
     * @param deputyClassId 代理类的class id
     * @param originColumns 源类属性名列表
     * @return 源类属性列表->代理类属性列表的哈希映射列表
     */
    private HashMap<String, String> getAttrNameHashMap(int originClassId, int deputyClassId, List<String> originColumns) {
        HashMap<String, String> attrNameHashMap = new HashMap<>();
        for (SwitchingTableItem switchingTableItem : MemConnect.getSwitchingT().switchingTableList) {
            if (switchingTableItem.oriId != originClassId || switchingTableItem.deputyId != deputyClassId) {
                continue;
            }

            for (String originColumn : originColumns) {
                if (switchingTableItem.oriAttr.equals(originColumn)) {
                    attrNameHashMap.put(originColumn, switchingTableItem.oriAttr);
                }
            }
        }
        return attrNameHashMap;
    }

    /**
     * 给定class id, 获取该源类对应的所有代理类（注：稍后放到memConnect中
     * @param classId 源类的class id
     * @return 该class id对应的所有代理类
     */
    private ArrayList<Integer> getDeputyByOriginId(int classId) {
        ArrayList<Integer> deputyIdList = new ArrayList<>();
        for (DeputyTableItem deputyTableItem : MemConnect.getDeputyt().deputyTableList) {
            if (deputyTableItem.originid == classId && !deputyTableItem.deputyrule[0].equals("5")) {
                deputyIdList.add(deputyTableItem.deputyid);
            }
        }
        return deputyIdList;
    }
}
