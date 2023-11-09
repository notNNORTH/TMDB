package edu.whu.tmdb.query.operations.impl;

import edu.whu.tmdb.storage.memory.MemManager;
import net.sf.jsqlparser.statement.Statement;

import java.io.IOException;
import java.util.ArrayList;

import edu.whu.tmdb.storage.memory.SystemTable.BiPointerTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.ClassTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.DeputyTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.ObjectTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.SwitchingTableItem;
import edu.whu.tmdb.query.operations.Exception.TMDBException;
import edu.whu.tmdb.query.operations.Drop;
import edu.whu.tmdb.query.operations.utils.MemConnect;

public class DropImpl implements Drop {

    private MemConnect memConnect;

    public DropImpl() {
        this.memConnect = MemConnect.getInstance(MemManager.getInstance());
    }

    @Override
    public boolean drop(Statement statement) throws TMDBException {
        return execute((net.sf.jsqlparser.statement.drop.Drop) statement);
    }

    public boolean execute(net.sf.jsqlparser.statement.drop.Drop drop) throws TMDBException {
        String tableName = drop.getName().getName();
        int classId = memConnect.getClassId(tableName);
        drop(classId);
        return true;
    }

    public void drop(int classId) {
        ArrayList<Integer> deputyClassIdList = new ArrayList<>();   // 存储该类对应所有代理类id

        // 1.删除ClassTableItem
        dropClassTable(classId);

        // 2.获取代理类id并在表中删除
        dropDeputyClassTable(classId, deputyClassIdList);

        // 3.删除 源类/对象<->代理类/对象 的双向关系表
        dropBiPointerTable(classId);

        // 4.删除switchingTable
        dropSwitchingTable(classId);

        // 5.删除已创建的源类对象
        dropObject(classId);

        // 6.递归删除代理类相关
        if(!deputyClassIdList.isEmpty()){
            for (Integer deputyClassId : deputyClassIdList) {
                drop(deputyClassId);
            }
        }
    }

    /**
     * 给定要删除的class id，删除系统表类表(class table)中的表项
     * @param classId 要删除的表对应的id
     */
    private void dropClassTable(int classId) {
        ArrayList<ClassTableItem> classItemList = new ArrayList<>();
        for (ClassTableItem classTableItem : MemConnect.getClasst().classTableList) {
            if(classTableItem.classid == classId){
                classItemList.add(classTableItem);
            }
        }
        for (ClassTableItem item : classItemList) {
            MemConnect.getClasst().classTableList.remove(item);
        }
    }

    /**
     * 删除系统表中的deputy table，并获取class id对应源类的代理类id
     * @param classId 源类id
     * @param deputyClassIdList 作为返回值，源类对应的代理类id列表
     */
    private void dropDeputyClassTable(int classId, ArrayList<Integer> deputyClassIdList) {
        ArrayList<DeputyTableItem> deputyCalssItemList = new ArrayList<>();
        for (DeputyTableItem deputyTableItem : MemConnect.getDeputyt().deputyTableList) {
            if(deputyTableItem.originid == classId){
                deputyClassIdList.add(deputyTableItem.deputyid);
                deputyCalssItemList.add(deputyTableItem);
            }
        }
        for(DeputyTableItem item : deputyCalssItemList){
            MemConnect.getDeputyt().deputyTableList.remove(item);
        }
    }

    /**
     * 删除系统表中的BiPointerTable
     * @param classId 源类id
     */
    private void dropBiPointerTable(int classId) {
        ArrayList<BiPointerTableItem> biPointerItemList = new ArrayList<>();
        for (BiPointerTableItem biPointerTableItem : memConnect.getBiPointerT().biPointerTableList) {
            if(biPointerTableItem.objectid == classId || biPointerTableItem.deputyobjectid == classId){
                biPointerItemList.add(biPointerTableItem);
            }
        }
        for (BiPointerTableItem item : biPointerItemList){
            memConnect.getBiPointerT().biPointerTableList.remove(item);
        }
    }

    /**
     * 删除系统表中的SwitchingTable
     * @param classId 源类id
     */
    private void dropSwitchingTable(int classId) {
        ArrayList<SwitchingTableItem> switchingTableList = new ArrayList<>();
        for (SwitchingTableItem switchingTableItem : memConnect.getSwitchingT().switchingTableList) {
            if(switchingTableItem.oriId == classId || switchingTableItem.deputyId == classId){
                switchingTableList.add(switchingTableItem);
            }
        }
        for(SwitchingTableItem temp : switchingTableList){
            memConnect.getSwitchingT().switchingTableList.remove(temp);
        }
    }

    /**
     * 删除源类具有的所有对象
     * @param classId 源类id
     */
    private void dropObject(int classId) {
        ArrayList<ObjectTableItem> objectList = new ArrayList<>();
        for (ObjectTableItem objectTableItem : memConnect.getTopt().objectTableList) {
            if(objectTableItem.classid == classId ){
                memConnect.DeleteTuple(objectTableItem.tupleid);
                objectList.add(objectTableItem);
            }
        }
        for(ObjectTableItem temp : objectList){
            memConnect.getTopt().objectTableList.remove(temp);
        }
    }

}
