/**
 * className: InsertImpl
 * Package: edu.whu.tmdb.query.operations.impl
 * Description: select的输出结果，对应传统数据库打印的结果，另外此处附带其他属性若干
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

        // 插入的TupleList
        SelectImpl select = new SelectImpl();

        // insert后面的values是一个select节点，获取values或者其它类型select的值
        SelectResult selectResult = select.select(insertStmt.getSelect());
        // tuplelist就是SelectResult中实际存储tuple部分
        TupleList tupleList = selectResult.getTpl();
        execute(table.getName(), attrNames, tupleList);
        return indexs;
    }

    /**
     *
     * @param tableName 表名/类名
     * @param columns 表/类所具有的属性列表
     * @param tupleList 我到现在也不知道这个是什么东西
     * @throws TMDBException
     * @throws IOException
     */
    public void execute(String tableName, List<String> columns, TupleList tupleList) throws TMDBException, IOException {
        int classId = memConnect.getClassId(tableName);         // 类id
        int attrNum = memConnect.getClassAttrnum(tableName);    // 属性的数量
        int[] attrId = getAttridList(classId, columns);         // 插入的属性对应的attrid列表
        for (int i = 0; i < tupleList.tuplelist.size(); i++) {
            indexs.add(insert(classId, columns, tupleList.tuplelist.get(i), attrNum, attrId));
        }
    }
    public void execute(int classId, List<String> columns, TupleList tupleList) throws TMDBException, IOException {
        int l = memConnect.getClassAttrnum(classId);
        int[] index = getAttridList(classId, columns);
        for (int i = 0; i < tupleList.tuplelist.size(); i++) {
            indexs.add(insert(classId,columns,tupleList.tuplelist.get(i),l,index));
        }
    }

    public int executeTuple(int classId, List<String> columns, Tuple tuple) throws TMDBException, IOException {
        int l = memConnect.getClassAttrnum(classId);
        int[] index = getAttridList(classId, columns);
        int insert = insert(classId, columns, tuple, l, index);
        indexs.add(insert);
        return insert;
    }

    /**
     * 用于获取插入位置对应的属性id (attrid)
     * @param classId insert对应的表id/类id
     * @param columns insert对应的属性名称列表
     * @return 属性名列表对应的attrid列表
     */
    private int[] getAttridList(int classId, List<String> columns) {
        int attrnum = memConnect.getClassAttrnum(classId);
        int[] attridList = new int[attrnum];

        // 遍历当前的ClassTableItem
        for (ClassTableItem classTableItem : memConnect.getClasst().classTableList) {
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
     * 分为几个阶段
     * 第一阶段插入tuple
     * @param classId
     * @param columns
     * @param tuple
     * @param l
     * @param index
     * @return
     * @throws TMDBException
     */
    private Integer insert(int classId, List<String> columns, Tuple tuple, int l, int[] index) throws TMDBException, IOException {
        // 插入tuple
        SelectImpl select = new SelectImpl();
        int tupleid = MemConnect.getTopt().maxTupleId++;
        Object[] temp = new Object[l];
        if(tuple.tuple.length != columns.size()){
            throw new TMDBException("Insert error: columns size not equals to tuple size");
        }
        for (int i = 0; i < index.length; i++) {
            temp[index[i]] = tuple.tuple[i];
        }
        tuple.classId = classId;
        tuple.tuple = temp;
        tuple.tupleHeader = tuple.tuple.length;
        int[] ids = new int[tuple.tupleHeader];
        Arrays.fill(ids,tupleid);
        tuple.tupleIds = ids;
        tuple.tupleId = tupleid;
        memConnect.InsertTuple(tuple);
        MemConnect.getTopt().objectTableList.add(new ObjectTableItem(classId,tupleid));

        //往代理类中进行插入
        ArrayList<Integer> pointTo = deputyTable(classId);
        //得到源类到每个代理类具体的属性对应关系。
        ArrayList<HashMap<Integer, Integer>> deputyAttr = getDeputyAttr(pointTo.size(), classId);
        //找到插入的类对应的代理类进行遍历
        for (int i = 0; i < pointTo.size(); i++) {
            int tempClassId = pointTo.get(i);
            HashMap<String, String> tempMap = trans(deputyAttr.get(i), classId, tempClassId);
            List<String> tempColumns = getInsertColumns(classId, tempClassId, tempMap, columns);
            Tuple tuple1 = getDeputyTuple(tempMap, tuple, columns);
            int i1 = executeTuple(tempClassId, tempColumns, tuple1);
            MemConnect.getBiPointerT().biPointerTableList.add(
                    new BiPointerTableItem(classId, tupleid, tempClassId, i1)
            );
        }

        LongestCommonSubSequence longestCommonSubSequence = new LongestCommonSubSequence();
        //现在由于多了基于轨迹相似度的代理类，因此，需要进行额外的逻辑处理
        //首先调用tJoinDeputySize方法得到是否当前类是否存在基于轨迹相似度的代理类，并得到代理类的id list
        ArrayList<Integer> tJoinDeputy = tJoinDeputySize(classId);
        //如果list非空就需要进一步判断
        if(!tJoinDeputy.isEmpty()) {
            //调用TrajTrans.getTraj方法将当前元祖的轨迹部分转化为List<Coordinate> traj1进行后续操作
            List<TrajEntry> traj1 = TrajTrans.getTraj((String) tuple.tuple[2]);
            //对代理类idlist进行遍历
            for (int i = 0; i < tJoinDeputy.size(); i++) {
                //得到当前代理类的id
                int deputyId = tJoinDeputy.get(i);
                //由于基于轨迹相似度的代理类需要两个源类，需要拿到另一个源类，这里使用getAnotherDeputy方法拿到
                int an = getAnotherDeputy(deputyId, classId);
                //通过select的getTable（classid）方法拿到另一个源类的所有tuple
                TupleList table = select.getTable(an);
                //遍历另一个源类的所有tuple
                for (int j = 0; j < table.tuplelist.size(); j++) {
                    //拿到另一个源类的当前tuple
                    Tuple tuple1 = table.tuplelist.get(j);
                    //通过TrajTrans.getTraj方法将当前元祖的轨迹部分转化为List<Coordinate> traj2进行后续操作
                    List<TrajEntry> traj2 = TrajTrans.getTraj((String) tuple1.tuple[2]);
                    //通过longestCommonSubSequence.getCommonSubsequence获取当前两个traj的公共子序列
                    List<Coordinate> commonSubsequence = longestCommonSubSequence.getCommonSubsequence(traj1, traj2, 3);
                    //如果子序列长度大于阈值，则需要在代理类中插入新的tuple
                    if (commonSubsequence.size() > 1) {
                        //新建临时tuple，这个tuple就是要往代理类中进行插入的tuple
                        //临时tuple除了轨迹部分存的和当前进行插入的tuple（方法形参里的不同，其它都相同）步骤和TJoinSelect一样
                        Tuple temp1 = new Tuple();
                        temp1.tupleId = tuple.tupleId;
                        temp1.tupleIds = tuple.tupleIds;
                        temp1.tuple = tuple.tuple;
                        //需要将得到的轨迹子序列，转换成string的形式，然后将tuple中轨迹部分设置为转换后的值
                        String temps = TrajTrans.getString(commonSubsequence);
                        temp1.tuple[2] = temps;
                        //调用一下方法在代理类中也插入新tuple
                        int i1 = executeTuple(deputyId, columns, temp1);
                        MemConnect.getBiPointerT().biPointerTableList.add(
                                new BiPointerTableItem(classId, tupleid, deputyId, i1)
                        );
                    }
                }
            }
        }
        return tupleid;
    }








    private int getAnotherDeputy(int deputyId, int classId) {
        for (int i = 0; i < MemConnect.getDeputyt().deputyTableList.size(); i++) {
            DeputyTableItem deputyTableItem = MemConnect.getDeputyt().deputyTableList.get(i);
            if("5".equals(deputyTableItem.deputyrule[0]) &&
            deputyTableItem.originid!=classId &&
            deputyTableItem.deputyid==deputyId){
                return deputyTableItem.originid;
            }
        }
        return -1;
    }

    /**
     * 将属性id与属性id的对应关系，转换成是属性名到属性名的对应关系。
     * @param map
     * @param classId
     * @param tempClassId
     * @return
     */
    private HashMap<String, String> trans(HashMap<Integer, Integer> map, int classId, int tempClassId) {
        HashMap<String,String> map2=new HashMap<>();
        HashMap<Integer,String> tempmap1=new HashMap<>();
        HashMap<Integer,String> tempmap2=new HashMap<>();
        for (int i = 0; i < MemConnect.getClasst().classTableList.size(); i++) {
            ClassTableItem classTableItem = MemConnect.getClasst().classTableList.get(i);
            if(classTableItem.classid==classId){
                tempmap1.put(classTableItem.attrid,classTableItem.attrname);
            }
            if(classTableItem.classid==tempClassId){
                tempmap2.put(classTableItem.attrid,classTableItem.attrname);
            }
        }
        for(int i:map.keySet()){
            map2.put(tempmap1.get(i),tempmap2.get(map.get(i)));
        }
        return map2;
    }

    private Tuple getDeputyTuple(HashMap<String, String> map, Tuple tuple, List<String> columns) {
        Tuple res=new Tuple();
        Object[] temp=new Object[map.size()];
        int i=0;
        for(String s:columns){
            if (map.containsKey(s)) {
                temp[i]=tuple.tuple[columns.indexOf(s)];
                i++;
            }
        }
        res.tuple=temp;
        return res;
    }

    private List<String> getInsertColumns(int classId, int tempClassId, HashMap<String, String> map, List<String> columns) {

        List<String> res=new ArrayList<>();
        for (String column :
                columns) {
            if(map.containsKey(column)){
                res.add(map.get(column));
            }
        }
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
     * 得到源类和每个对应代理类的属性id map
     * @param deputySize
     * @param oriId
     * @return
     */
    private ArrayList<HashMap<Integer,Integer>> getDeputyAttr(int deputySize, int oriId){
        int i=0;
        int c=-1;
        ArrayList<HashMap<Integer,Integer>> res=new ArrayList<>();
        while(i< MemConnect.getSwitchingT().switchingTableList.size()){
            SwitchingTableItem switchingTableItem = MemConnect.getSwitchingT().switchingTableList.get(i);
            if(switchingTableItem.oriId==oriId){
                c=switchingTableItem.deputyId;
                HashMap<Integer,Integer> map=new HashMap<>();
                while(i< MemConnect.getSwitchingT().switchingTableList.size() &&
                        MemConnect.getSwitchingT().switchingTableList.get(i).deputyId==c){
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
     * 获取源类对应的代理类
     * @param classId
     * @return
     */
    private ArrayList<Integer> deputyTable(int classId) {
        ArrayList<Integer> deputy=new ArrayList<>();
        for (int i = 0; i < MemConnect.getDeputyt().deputyTableList.size(); i++) {
            DeputyTableItem deputyTableItem = MemConnect.getDeputyt().deputyTableList.get(i);
            if(deputyTableItem.originid==classId && !deputyTableItem.deputyrule[0].equals("5")){
                deputy.add(deputyTableItem.deputyid);
            }
        }
        return deputy;
    }

    private ArrayList<Integer> tJoinDeputySize(int classId) {
        ArrayList<Integer> deputy=new ArrayList<>();
        for (int i = 0; i < MemConnect.getDeputyt().deputyTableList.size(); i++) {
            DeputyTableItem deputyTableItem = MemConnect.getDeputyt().deputyTableList.get(i);
            if(deputyTableItem.originid==classId && deputyTableItem.deputyrule[0].equals("5")){
                deputy.add(deputyTableItem.deputyid);
            }
        }
        return deputy;
    }

    public int tupleInsert(int classId, Tuple tuple, boolean hasDeputy){

        int tupleid = MemConnect.getTopt().maxTupleId++;
        tuple.tupleHeader=tuple.tuple.length;
        int[] ids=new int[tuple.tupleHeader];
        Arrays.fill(ids,tupleid);
        tuple.tupleId=tupleid;
        memConnect.InsertTuple(tuple);
        MemConnect.getTopt().objectTableList.add(new ObjectTableItem(classId,tupleid));
        if(hasDeputy){

        }
        return 1;
    }
}
