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
        int[] attrId = memConnect.getAttridList(classId, columns);         // 插入的属性对应的attrid列表
        for (Tuple tuple : tupleList.tuplelist) {
            indexs.add(insert(classId, columns, tuple, attrNum, attrId));
        }
    }



    public void execute(int classId, List<String> columns, TupleList tupleList) throws TMDBException, IOException {
        int l = memConnect.getClassAttrnum(classId);
        int[] index = memConnect.getAttridList(classId, columns);
        for (int i = 0; i < tupleList.tuplelist.size(); i++) {
            indexs.add(insert(classId,columns,tupleList.tuplelist.get(i),l,index));
        }
    }


    /**
     *
     * @param classId 不是，按道理来说这里应该是代理类的id啊
     * @param columns 代理类的属性名列表
     * @param tuple 要insert的元组tuple
     * @return
     * @throws TMDBException
     * @throws IOException
     */
    public int executeTuple(int classId, List<String> columns, Tuple tuple) throws TMDBException, IOException {
        int attrNum = memConnect.getClassAttrnum(classId);
        int[] attridList = memConnect.getAttridList(classId, columns);
        int insert = insert(classId, columns, tuple, attrNum, attridList);
        indexs.add(insert);
        return insert;
    }


    /**
     * 分为几个阶段
     * 第一阶段插入tuple
     * @param classId 插入表/类对应的id
     * @param columns 表/类所具有的属性名列表
     * @param tuple 要插入的元组
     * @param attrNum 元组包含的属性数量
     * @param attrId 插入属性对应的attrId列表
     * @return
     * @throws TMDBException
     */
    private Integer insert(int classId, List<String> columns, Tuple tuple, int attrNum, int[] attrId) throws TMDBException, IOException {
        // 插入tuple
        // SelectImpl select = new SelectImpl();
        int tupleid = MemConnect.getTopt().maxTupleId++;    // 获取新插入元组的id


        // 将tuple转换为可插入的模样
        Object[] temp = new Object[attrNum];
        if (tuple.tuple.length != columns.size()){
            throw new TMDBException("Insert error: columns size doesn't match tuple size");
        }
        for (int i = 0; i < attrId.length; i++) {
            temp[attrId[i]] = tuple.tuple[i];
        }
        tuple.setTuple(tuple.tuple.length, tupleid, classId, temp);

        memConnect.InsertTuple(tuple);
        MemConnect.getTopt().objectTableList.add(new ObjectTableItem(classId, tupleid));

        //往代理类中进行插入
        ArrayList<Integer> pointTo = deputyTable(classId);      // 获取给定class id的所有代理类id
        //得到源类到每个代理类具体的属性对应关系。
        ArrayList<HashMap<Integer, Integer>> deputyAttr = getDeputyAttr(pointTo.size(), classId);
        //找到插入的类对应的代理类进行遍历
        for (int i = 0; i < pointTo.size(); i++) {
            int tempClassId = pointTo.get(i);   // 获取代理类id
            HashMap<String, String> tempMap = trans(deputyAttr.get(i), classId, tempClassId);       // 这他妈有什么用呢？既然需要为什么刚刚不一起拿出来算了
            List<String> tempColumns = getInsertColumns(classId, tempClassId, tempMap, columns);    // 根据源类属性名列表获取代理类属性名列表
            Tuple tuple1 = getDeputyTuple(tempMap, tuple, columns);     // 将插入源类的元组tuple转换为插入代理类的元组tuple1
            int i1 = executeTuple(tempClassId, tempColumns, tuple1);
            MemConnect.getBiPointerT().biPointerTableList.add(
                    new BiPointerTableItem(classId, tupleid, tempClassId, i1)
            );
        }
        /*
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
        }*/
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
     * @param deputySize 源类对应代理类的数量
     * @param oriId 源类的id
     * @return 存储哈希映射的列表
     */
    private ArrayList<HashMap<Integer,Integer>> getDeputyAttr(int deputySize, int oriId){
        int i=0;
        int c=-1;
        ArrayList<HashMap<Integer,Integer>> res=new ArrayList<>();
        while(i< MemConnect.getSwitchingT().switchingTableList.size()){     // 遍历所有交换表项
            SwitchingTableItem switchingTableItem = MemConnect.getSwitchingT().switchingTableList.get(i);
            if(switchingTableItem.oriId==oriId){
                c=switchingTableItem.deputyId;      // 获取源类id对应的代理类id
                HashMap<Integer, Integer> map = new HashMap<>();        // map中存储源类属性id到该代理类属性id之间的所有映射
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
     * 给定class id, 获取该源类对应的所有代理类
     * @param classId 源类的class id
     * @return 该class id对应的所有代理类
     */
    private ArrayList<Integer> deputyTable(int classId) {
        ArrayList<Integer> deputy = new ArrayList<>();
        for (int i = 0; i < MemConnect.getDeputyt().deputyTableList.size(); i++) {
            DeputyTableItem deputyTableItem = MemConnect.getDeputyt().deputyTableList.get(i);
            if(deputyTableItem.originid == classId && !deputyTableItem.deputyrule[0].equals("5")){
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
