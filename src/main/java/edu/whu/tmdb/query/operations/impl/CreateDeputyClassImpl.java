package edu.whu.tmdb.query.operations.impl;

import edu.whu.tmdb.query.operations.Exception.ErrorList;
import edu.whu.tmdb.storage.memory.MemManager;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.HashSet;
import java.util.List;

import edu.whu.tmdb.storage.memory.SystemTable.BiPointerTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.ClassTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.DeputyTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.SwitchingTableItem;
import edu.whu.tmdb.storage.memory.Tuple;
import edu.whu.tmdb.query.operations.CreateDeputyClass;
import edu.whu.tmdb.query.operations.Exception.TMDBException;
import edu.whu.tmdb.query.operations.utils.MemConnect;
import edu.whu.tmdb.query.operations.utils.SelectResult;


public class CreateDeputyClassImpl implements CreateDeputyClass {
    private final MemConnect memConnect;

    public CreateDeputyClassImpl() { this.memConnect = MemConnect.getInstance(MemManager.getInstance()); }

    @Override
    public boolean createDeputyClass(Statement stmt) throws TMDBException, IOException {
        return execute((net.sf.jsqlparser.statement.create.deputyclass.CreateDeputyClass) stmt);
    }

    public boolean execute(net.sf.jsqlparser.statement.create.deputyclass.CreateDeputyClass stmt) throws TMDBException, IOException {
        // 1.获取代理类名、代理类型、select元组
        String deputyClassName = stmt.getDeputyClass().toString();  // 代理类名
        if (memConnect.classExist(deputyClassName)) {
            throw new TMDBException(ErrorList.TABLE_ALREADY_EXISTS, deputyClassName);
        }
        int deputyType = getDeputyType(stmt);   // 代理类型
        Select selectStmt = stmt.getSelect();
        SelectResult selectResult = getSelectResult(selectStmt);

        // 2.执行代理类创建
        return createDeputyClassStreamLine(selectResult, deputyType, deputyClassName);
    }


    public boolean createDeputyClassStreamLine(SelectResult selectResult, int deputyType, String deputyClassName) throws TMDBException, IOException {
        int deputyId = createDeputyClass(deputyClassName, selectResult, deputyType);
        insertDeputyTable(selectResult.getClassName(), deputyType, deputyId);
        insertTuple(selectResult, deputyId);
        return true;
    }

    /**
     * 给定创建代理类语句，返回代理规则
     * @param stmt 创建代理类语句
     * @return 代理规则
     */
    private int getDeputyType(net.sf.jsqlparser.statement.create.deputyclass.CreateDeputyClass stmt) {
        switch (stmt.getType().toLowerCase(Locale.ROOT)) {
            case "selectdeputy":    return 0;
            case "joindeputy":      return 1;
            case "uniondeputy":     return 2;
            case "groupbydeputy":   return 3;
        }
        return -1;
    }

    /**
     * 给定查询语句，返回select查询执行结果（创建deputyclass后面的select语句中的selectResult）
     * @param selectStmt select查询语句
     * @return 查询执行结果（包含所有满足条件元组）
     */
    private SelectResult getSelectResult(Select selectStmt) throws TMDBException, IOException {
        SelectImpl selectExecutor = new SelectImpl();
        return selectExecutor.select(selectStmt);
    }

    //第一步，创建代理类，代理类的classtype设置为de
    //同时，在switchingtable中插入源属性到代理属性的映射
    private int createDeputyClass(String deputyClassName, SelectResult selectResult, int deputyRule) throws TMDBException {
        MemConnect.getClasst().maxid++;
        int classId = MemConnect.getClasst().maxid;         // 代理类的id
        int attrNum = selectResult.getAttrid().length;      // 代理类的长度
        for (int i = 0; i < selectResult.getAttrid().length; i++) {
            // 1.新建classTableItem
            MemConnect.getClasst().classTableList.add(
                    new ClassTableItem(deputyClassName, classId, attrNum, selectResult.getAttrid()[i],
                            selectResult.getAttrname()[i], selectResult.getType()[i], "de", ""));
            // 2.新建switchingTableItem
            String className = selectResult.getClassName()[i];
            int oriId = memConnect.getClassId(className);
            int oriAttrId = getOriAttrId(oriId, selectResult.getAlias()[i]);
            MemConnect.getSwitchingT().switchingTableList.add(
                    new SwitchingTableItem(oriId, oriAttrId, selectResult.getAlias()[i], classId,
                            i, selectResult.getAttrname()[i], deputyRule + "")
            );
        }
        return classId;
    }

    /**
     * get the origin class classid
     *
     * @param oriId
     * @param alias
     * @return
     */
    private int getOriAttrId(int oriId, String alias) {
        for (int i = 0; i < MemConnect.getClasst().classTableList.size(); i++) {
            ClassTableItem classTableItem = MemConnect.getClasst().classTableList.get(i);
            if (classTableItem.classid == oriId && classTableItem.attrname.equals(alias)) {
                return classTableItem.attrid;
            }
        }
        return -1;
    }

    /**
     * 第二步，在deputytable中插入具体的tuple
     *
     * @param className  deputy class's className
     * @param deputyType deputy class's deputytype
     * @param deputyId
     * @throws TMDBException
     */
    public void insertDeputyTable(String[] className, int deputyType, int deputyId) throws TMDBException {
        HashSet<String> collect = Arrays.stream(className).collect(Collectors.toCollection(HashSet::new));
        for (String s :
                collect) {
            int oriId = memConnect.getClassId(s);
            MemConnect.getDeputyt().deputyTableList.add(
                    new DeputyTableItem(oriId, deputyId, new String[]{deputyType + ""})
            );
        }
    }

    //第三步，在ObjectTable中插入实际值
    private void insertTuple(SelectResult selectResult, int deputyId) throws TMDBException, IOException {
        InsertImpl insert = new InsertImpl();
        List<String> columns = Arrays.asList(selectResult.getAttrname());
        for (int i = 0; i < selectResult.getTpl().tuplelist.size(); i++) {
            Tuple tuple = selectResult.getTpl().tuplelist.get(i);
            int deputyTupleId = insert.execute(deputyId, columns, new Tuple(tuple.tuple));
            HashSet<Integer> origin = getOriginClass(selectResult);
            for (int o :
                    origin) {
                int classId = memConnect.getClassId(selectResult.getClassName()[o]);
                int oriTupleId = tuple.tupleIds[o];
                MemConnect.getBiPointerT().biPointerTableList.add(
                        new BiPointerTableItem(classId, oriTupleId, deputyId, deputyTupleId)
                );
            }
        }
    }

    private HashSet<Integer> getOriginClass(SelectResult selectResult) {
        ArrayList<String> collect = Arrays.stream(selectResult.getClassName()).collect(Collectors.toCollection(ArrayList::new));
        HashSet<String> collect1 = Arrays.stream(selectResult.getClassName()).collect(Collectors.toCollection(HashSet::new));
        HashSet<Integer> res = new HashSet<>();
        for (String s : collect1) {
            res.add(collect.indexOf(s));
        }
        return res;
    }


}
