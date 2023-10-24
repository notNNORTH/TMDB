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
        this.memConnect=MemConnect.getInstance(MemManager.getInstance());
    }

    @Override
    public boolean drop(Statement statement) throws TMDBException {
        return execute((net.sf.jsqlparser.statement.drop.Drop) statement);
    }

    public boolean execute(net.sf.jsqlparser.statement.drop.Drop drop) throws TMDBException {
        String dropTable=drop.getName().getName();
        int classId=memConnect.getClassId(dropTable);
        drop(classId);
        return true;
    }

    public void drop(int classId){
        ArrayList<ClassTableItem> tempC=new ArrayList<>();
        for (int i = 0; i < MemConnect.getClasst().classTableList.size(); i++) {
            ClassTableItem classTableItem = MemConnect.getClasst().classTableList.get(i);
            if(classTableItem.classid==classId){
                tempC.add(classTableItem);
            }
        }
        for (ClassTableItem temp :
                tempC) {
            MemConnect.getClasst().classTableList.remove(temp);
        }
        ArrayList<DeputyTableItem> tempD=new ArrayList<>();
        ArrayList<Integer> toDrop=new ArrayList<>();
        for (int i = 0; i < memConnect.getDeputyt().deputyTableList.size(); i++) {
            DeputyTableItem deputyTableItem = memConnect.getDeputyt().deputyTableList.get(i);
            if(deputyTableItem.originid==classId){
                toDrop.add(deputyTableItem.deputyid);
                tempD.add(deputyTableItem);
            }
        }
        for(DeputyTableItem temp: tempD){
            MemConnect.getDeputyt().deputyTableList.remove(temp);
        }
        ArrayList<BiPointerTableItem> tempB=new ArrayList<>();
        for (int i = 0; i < memConnect.getBiPointerT().biPointerTableList.size(); i++) {
            BiPointerTableItem biPointerTableItem = memConnect.getBiPointerT().biPointerTableList.get(i);
            if(biPointerTableItem.objectid==classId || biPointerTableItem.deputyobjectid==classId){
                tempB.add(biPointerTableItem);
            }
        }
        for(BiPointerTableItem temp:tempB){
            memConnect.getBiPointerT().biPointerTableList.remove(temp);
        }
        ArrayList<SwitchingTableItem> tempS=new ArrayList<>();
        for (int i = 0; i < memConnect.getSwitchingT().switchingTableList.size(); i++) {
            SwitchingTableItem switchingTableItem = memConnect.getSwitchingT().switchingTableList.get(i);
            if(switchingTableItem.oriId==classId || switchingTableItem.deputyId==classId){
                tempS.add(switchingTableItem);
            }
        }
        for(SwitchingTableItem temp:tempS){
            memConnect.getSwitchingT().switchingTableList.remove(temp);
        }
        ArrayList<ObjectTableItem> tempT=new ArrayList<>();
        for (int i = 0; i < memConnect.getTopt().objectTableList.size(); i++) {
            ObjectTableItem objectTableItem = memConnect.getTopt().objectTableList.get(i);
            if(objectTableItem.classid==classId ){
                memConnect.DeleteTuple(objectTableItem.tupleid);
                tempT.add(objectTableItem);
            }
        }
        for(ObjectTableItem temp:tempT){
            memConnect.getTopt().objectTableList.remove(temp);
        }
        if(toDrop.isEmpty()){
            return;
        }
        for (int i = 0; i < toDrop.size(); i++) {
            drop(toDrop.get(i));
        }
    }

    //DROP CLASS asd;
    //3,asd
//    public boolean drop(String[]p) throws TMDBException {
//        List<DeputyTableItem> dti;
//        dti = Drop1(p);
//        for(DeputyTableItem item:dti){
//            memConnect.getDeputyt().deputyTable.remove(item);
//        }
//        return  true;
//    }
//
//    private List<DeputyTableItem> Drop1(String[] p) throws TMDBException {
//        String classname = p[1];
//        int classid = memConnect.getClassId(p[1]);
//        //找到classid顺便 清除类表和switch表
//
//        for (Iterator it1 = memConnect.getClasst().classTable.iterator(); it1.hasNext();) {
//            ClassTableItem item =(ClassTableItem) it1.next();
//            if (item.classname.equals(classname) ){
//                classid = item.classid;
//                for(Iterator it = memConnect.getSwitchingT().switchingTable.iterator(); it.hasNext();) {
//                    SwitchingTableItem item2 =(SwitchingTableItem) it.next();
////                    if (item2.attr.equals( item.attrname)||item2.deputy .equals( item.attrname)){
////                        it.remove();
////                    }
//                }
//                it1.remove();
//            }
//        }
//        //清元组表同时清了bi
//        MemConnect.OandB ob2 = new MemConnect.OandB();
//        DeleteImpl delete=new DeleteImpl(memConnect);
//        for(ObjectTableItem item1: memConnect.getTopt().objectTable){
//            if(item1.classid == classid){
//                MemConnect.OandB ob = delete.DeletebyID(item1.tupleid);
//                for(ObjectTableItem obj:ob.o){
//                    ob2.o.add(obj);
//                }
//                for(BiPointerTableItem bip:ob.b){
//                    ob2.b.add(bip);
//                }
//            }
//        }
//        for(ObjectTableItem obj:ob2.o){
//            memConnect.getTopt().objectTable.remove(obj);
//        }
//        for(BiPointerTableItem bip:ob2.b) {
//            memConnect.getBiPointerT().biPointerTable.remove(bip);
//        }
//
//        //清deputy
//        List<DeputyTableItem> dti = new ArrayList<>();
//        for(DeputyTableItem item3: memConnect.getDeputyt().deputyTable){
//            if(item3.deputyid == classid){
//                if(!dti.contains(item3))
//                    dti.add(item3);
//            }
//            if(item3.originid == classid){
//                //删除代理类
//                String[]s = p.clone();
//                List<String> sname = new ArrayList<>();
//                for(ClassTableItem item5: memConnect.getClasst().classTable) {
//                    if (item5.classid == item3.deputyid) {
//                        sname.add(item5.classname);
//                    }
//                }
//                for(String item4: sname){
//
//                    s[1] = item4;
//                    List<DeputyTableItem> dti2 = Drop1(s);
//                    for(DeputyTableItem item8:dti2){
//                        if(!dti.contains(item8))
//                            dti.add(item8);
//                    }
//
//                }
//                if(!dti.contains(item3))
//                    dti.add(item3);
//            }
//        }
//        return dti;
//
//    }
}
