package edu.whu.tmdb.query.operations.impl;

import edu.whu.tmdb.query.operations.Exception.ErrorList;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.update.UpdateSet;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import edu.whu.tmdb.storage.memory.MemManager;
import edu.whu.tmdb.storage.memory.SystemTable.BiPointerTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.SwitchingTableItem;
import edu.whu.tmdb.storage.memory.Tuple;
import edu.whu.tmdb.storage.memory.TupleList;
import edu.whu.tmdb.query.operations.Exception.TMDBException;
import edu.whu.tmdb.query.operations.Select;
import edu.whu.tmdb.query.operations.Update;
import edu.whu.tmdb.query.operations.utils.MemConnect;
import edu.whu.tmdb.query.operations.utils.SelectResult;

public class UpdateImpl implements Update {

    private final MemConnect memConnect;

    public UpdateImpl() {
        this.memConnect = MemConnect.getInstance(MemManager.getInstance());
    }

    @Override
    public void update(Statement stmt) throws JSQLParserException, TMDBException, IOException {
        execute((net.sf.jsqlparser.statement.update.Update) stmt);
    }

    // UPDATE Song SET type = ‘jazz’ WHERE songId = 100;
    // OPT_CREATE_UPDATE，Song，type，“jazz”，songId，=，100
    // 0                  1     2      3        4      5  6
    public void execute(net.sf.jsqlparser.statement.update.Update updateStmt) throws JSQLParserException, TMDBException, IOException {
        // 1.获取符合where条件的所有元组
        String updateTableName = updateStmt.getTable().getName();
        String sql = "select * from " + updateTableName + " where " + updateStmt.getWhere().toString() + ";";
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(sql.getBytes());
        net.sf.jsqlparser.statement.select.Select parse = (net.sf.jsqlparser.statement.select.Select) CCJSqlParserUtil.parse(byteArrayInputStream);
        Select select = new SelectImpl();
        SelectResult selectResult = select.select(parse);


        ArrayList<UpdateSet> updateSetStmts = updateStmt.getUpdateSets();    // update语句中set字段列表
        int[] indexs = new int[updateSetStmts.size()];      // update中set语句修改的属性->类表中属性的映射关系
        String[] toUpdate = new String[updateSetStmts.size()];
        Arrays.fill(indexs, -1);
        Object[] updateValue = new Object[updateSetStmts.size()];
        for (int i = 0; i < updateSetStmts.size(); i++) {
            UpdateSet updateSet = updateSetStmts.get(i);
            for (int j = 0; j < selectResult.getAttrname().length; j++) {
                if (updateSet.getColumns().get(0).getColumnName().equals(selectResult.getAttrname()[j])) {
                    indexs[i] = j;      // set语句中的第i个对应于源类中第j个属性
                    toUpdate[i] = selectResult.getAttrname()[j];    // set语句中的第i个对应于源类中第j个属性的名字
                    Object after = new Object();
                    if (updateSet.getExpressions().get(0) instanceof StringValue) {
                        after = ((StringValue) updateSet.getExpressions().get(0)).getValue();
                    } else {
                        after = updateSet.getExpressions().get(0).toString();
                    }
                    updateValue[i] = after;     // set语句中的第i个对应于源类中第j个属性修改后的值
                    break;
                }
            }
            if (indexs[i] == -1)
                throw new TMDBException(ErrorList.COLUMN_NAME_DOES_NOT_EXIST, updateSet.getColumns().get(0).getColumnName());
        }
        int classId = memConnect.getClassId(updateTableName);
        update(selectResult.getTpl(), indexs, updateValue, classId);
    }

    public void update(TupleList tupleList, int[] indexs, Object[] updateValue, int classId) throws TMDBException {
        ArrayList<Integer> insertIndex = new ArrayList<>();
        for (Tuple tuple : tupleList.tuplelist) {
            for (int i = 0; i < indexs.length; i++) {
                tuple.tuple[indexs[i]] = updateValue[i];
            }
            memConnect.UpateTuple(tuple, tuple.getTupleId());
            insertIndex.add(tuple.getTupleId());
        }
        ArrayList<Integer> toUpdate = new ArrayList<>();
        for (int i = 0; i < MemConnect.getBiPointerT().biPointerTableList.size(); i++) {
            BiPointerTableItem biPointerTableItem = MemConnect.getBiPointerT().biPointerTableList.get(i);
            if (insertIndex.contains(biPointerTableItem.objectid)) {
                toUpdate.add(biPointerTableItem.deputyobjectid);
            }
        }
        if (toUpdate.isEmpty()) {
            return;
        }
        List<Integer> collect = Arrays.stream(indexs).boxed().collect(Collectors.toList());
        HashMap<Integer, ArrayList<Integer>> map = new HashMap<>();
        HashMap<Integer, ArrayList<Object>> map2 = new HashMap<>();
        for (int i = 0; i < MemConnect.getSwitchingT().switchingTableList.size(); i++) {
            SwitchingTableItem switchingTableItem = MemConnect.getSwitchingT().switchingTableList.get(i);
            if (switchingTableItem.oriId == classId && collect.contains(switchingTableItem.oriAttrid)) {
                if (!map.containsKey(switchingTableItem.deputyId)) {
                    map.put(switchingTableItem.deputyId, new ArrayList<>());
                    map2.put(switchingTableItem.deputyId, new ArrayList<>());
                }
                map.get(switchingTableItem.deputyId).add(switchingTableItem.deputyAttrId);
                int tempIndex = collect.indexOf(switchingTableItem.oriAttrid);
                map2.get(switchingTableItem.deputyId).add(updateValue[tempIndex]);
            }
        }
        TupleList tupleList1 = new TupleList();
        for (Integer integer : toUpdate) {
            Tuple tuple = memConnect.GetTuple(integer);
            tupleList1.addTuple(tuple);
        }
        for (int i : map.keySet()) {
            TupleList tupleList2 = new TupleList();
            for (int j = 0; j < tupleList1.tuplelist.size(); j++) {
                Tuple tuple = tupleList1.tuplelist.get(j);
                if (tuple.classId == i) {
                    tupleList2.addTuple(tuple);
                }
            }
            int[] nextIndexs = map.get(i).stream().mapToInt(Integer -> Integer).toArray();
            Object[] nextUpdate = map2.get(i).toArray();
            update(tupleList2, nextIndexs, nextUpdate, i);
        }
    }
}
