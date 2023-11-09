package edu.whu.tmdb.query.operations.impl;

import au.edu.rmit.bdm.Torch.base.model.Coordinate;
import au.edu.rmit.bdm.Torch.base.model.TrajEntry;
import au.edu.rmit.bdm.Torch.base.model.Trajectory;
import au.edu.rmit.bdm.Torch.queryEngine.model.SearchWindow;
import edu.whu.tmdb.storage.memory.MemManager;
import edu.whu.tmdb.query.operations.torch.TorchConnect;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.statement.select.PlainSelect;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import edu.whu.tmdb.query.operations.Exception.TMDBException;
import edu.whu.tmdb.query.operations.Select;
import edu.whu.tmdb.query.operations.utils.Formula;
import edu.whu.tmdb.query.operations.utils.MemConnect;
import edu.whu.tmdb.query.operations.utils.SelectResult;
import edu.whu.tmdb.query.operations.utils.traj.TrajTrans;
import edu.whu.tmdb.storage.memory.Tuple;
import edu.whu.tmdb.storage.memory.TupleList;

public class Where {
    private MemConnect memConnect;

    public Where() throws TMDBException, IOException {
        this.memConnect = MemConnect.getInstance(MemManager.getInstance());
    }

    Formula formula = new Formula();

    public SelectResult where(PlainSelect plainSelect, SelectResult selectResult) throws TMDBException, IOException {
        execute(plainSelect.getWhere(), selectResult);
        return selectResult;
    }

    /**
     * 核心类，将整个where语法树进行后续遍历
     * @param expression where关键字后面的表达式
     * @param selectResult 待筛选的selectResult
     * @return 筛选之后的selectResult
     */
    public SelectResult execute(Expression expression, SelectResult selectResult) throws TMDBException, IOException {
        SelectResult result = new SelectResult();
        if (selectResult.getTpl().tuplelist.isEmpty()) {
            return selectResult;
        }
        String opt = expression.getClass().getSimpleName();
        switch (opt){
            case "OrExpression":
                result = orExpression((OrExpression) expression, selectResult); break;
            case "AndExpression":
                result = andExpression((AndExpression) expression, selectResult); break;
            case "InExpression":
                result = inExpression((InExpression) expression, selectResult); break;
            case "EqualsTo":
                result = equalsToExpression((EqualsTo) expression, selectResult); break;
            case "MinorThan":
                result = minorThan((MinorThan) expression, selectResult); break;
            case "GreaterThan":
                result = greaterThan((GreaterThan) expression, selectResult); break;
            case "Function":
                result = function((Function)expression, selectResult); break;
        }
        return result;
    }

    private SelectResult function(Function expression, SelectResult selectResult)  {
        String name = expression.getName();
        switch(name.toLowerCase()){
            case "st_within": return range(expression,selectResult);
            case "st_intersect": return path(expression,selectResult);
            case "st_contain": return strictPath(expression,selectResult);
            case "st_similarity": return topK(expression,selectResult);
        }
        return null;

    }

    private SelectResult topK(Function expression, SelectResult selectResult)  {
        List<Expression> expressions = expression.getParameters().getExpressions();

        List<Expression> list = ((Function) expressions.get(0)).getParameters().getExpressions();
        Trajectory<TrajEntry> trajEntries = new Trajectory<>();
        for (int i = 0; i < list.size(); i+=2) {
            Double lat = Double.parseDouble(list.get(i+1).toString());
            Double lng = Double.parseDouble(list.get(i).toString());
            Coordinate coordinate = new Coordinate(lat, lng);
            trajEntries.add(coordinate);
        }
        int k=Integer.parseInt(expressions.get(1).toString());
        if(expressions.size()==2) {
            List<Trajectory<TrajEntry>> trajectories = TorchConnect.getTorchConnect().topkQuery(trajEntries, k);
            return TrajTrans.getSelectResultByTrajList(trajectories, selectResult);
        }
        else{
            String similarityFunction=expressions.get(2).toString();
            List<Trajectory<TrajEntry>> trajectories = TorchConnect.getTorchConnect().topkQuery(trajEntries, k,similarityFunction);
            return TrajTrans.getSelectResultByTrajList(trajectories,selectResult);
        }
    }

    private SelectResult strictPath(Function expression, SelectResult selectResult) {
        List<Expression> expressions = expression.getParameters().getExpressions();
        Expression para = expressions.get(0);
        if(para.getClass().getSimpleName().toLowerCase().equals("function")){
            List<Expression> list = ((Function) para).getParameters().getExpressions();
            Trajectory<TrajEntry> trajEntries = new Trajectory<>();
            for (int i = 0; i < list.size(); i+=2) {
                Double lat = Double.parseDouble(list.get(i+1).toString());
                Double lng = Double.parseDouble(list.get(i).toString());
                Coordinate coordinate = new Coordinate(lat, lng);
                trajEntries.add(coordinate);
            }
            List<Trajectory<TrajEntry>> trajectories = TorchConnect.getTorchConnect().strictPathQuery(trajEntries);
            return TrajTrans.getSelectResultByTrajList(trajectories,selectResult);
        }
        else{
            List<Trajectory<TrajEntry>> trajectories = TorchConnect.getTorchConnect().strictPathQuery(para.toString());
            return TrajTrans.getSelectResultByTrajList(trajectories,selectResult);

        }
    }

    private SelectResult path(Function expression, SelectResult selectResult) {
        List<Expression> expressions = expression.getParameters().getExpressions();
        Expression para = expressions.get(0);
        if(para.getClass().getSimpleName().toLowerCase().equals("function")){
            List<Expression> list = ((Function) para).getParameters().getExpressions();
            Trajectory<TrajEntry> trajEntries = new Trajectory<>();
            for (int i = 0; i < list.size(); i+=2) {
                Double lat = Double.parseDouble(list.get(i+1).toString());
                Double lng = Double.parseDouble(list.get(i).toString());
                Coordinate coordinate = new Coordinate(lat, lng);
                trajEntries.add(coordinate);
            }
            List<Trajectory<TrajEntry>> trajectories = TorchConnect.getTorchConnect().pathQuery(trajEntries);
            return TrajTrans.getSelectResultByTrajList(trajectories,selectResult);
        }
        else{
            List<Trajectory<TrajEntry>> trajectories = TorchConnect.getTorchConnect().pathQuery(para.toString());
            return TrajTrans.getSelectResultByTrajList(trajectories,selectResult);

        }
    }

    private SelectResult range(Function expression, SelectResult selectResult) {
        List<Expression> expressions = expression.getParameters().getExpressions();
        Function searchWindow = (Function) expressions.get(0);
        List<Expression> swPara = searchWindow.getParameters().getExpressions();
        if(swPara.get(1).getClass().getSimpleName().toLowerCase().equals("function")){
            List<Expression> coor1 = ((Function) swPara.get(0)).getParameters().getExpressions();
            List<Expression> coor2 = ((Function) swPara.get(1)).getParameters().getExpressions();
            Double lat1=Double.parseDouble(coor1.get(1).toString());
            Double lng1=Double.parseDouble(coor1.get(0).toString());
            Coordinate coordinate1 = new Coordinate(lat1, lng1);
            Double lat2=Double.parseDouble(coor2.get(1).toString());
            Double lng2=Double.parseDouble(coor2.get(0).toString());
            Coordinate coordinate2 = new Coordinate(lat2, lng2);
            TorchConnect torchConnect = TorchConnect.getTorchConnect();
            SearchWindow searchWindow1 = new SearchWindow(coordinate1, coordinate2);
            List<Trajectory<TrajEntry>> trajectories = torchConnect.rangeQuery(searchWindow1);
            return TrajTrans.getSelectResultByTrajList(trajectories,selectResult);
        }
        TorchConnect torchConnect = TorchConnect.getTorchConnect();
        Function coordinate = (Function) swPara.get(0);
        List<Expression> coor = coordinate.getParameters().getExpressions();
        Double lat=Double.parseDouble(coor.get(1).toString());
        Double lng=Double.parseDouble(coor.get(0).toString());
        double radius = Double.parseDouble(swPara.get(1).toString());
        Coordinate coordinate1 = new Coordinate(lat, lng);
        List<Trajectory<TrajEntry>> trajectories = torchConnect.rangeQuery(new SearchWindow(coordinate1, radius));
        return TrajTrans.getSelectResultByTrajList(trajectories,selectResult);
    }



    public SelectResult andExpression(AndExpression expression, SelectResult selectResult) throws TMDBException, IOException {
        Expression left=expression.getLeftExpression();
        Expression right=expression.getRightExpression();
        SelectResult selectResult1=execute(left,selectResult);
        SelectResult selectResult2=execute(right,selectResult);
        HashSet<Tuple> selectResultSet1=getTupleSet(selectResult1);
        HashSet<Tuple> selectResultSet2=getTupleSet(selectResult2);
        HashSet<Tuple> overlap=new HashSet<>();
        //将两个条件都满足的Tuple加入overlap中
        for(Tuple tuple:selectResultSet2){
            if(selectResultSet1.contains(tuple)) overlap.add(tuple);
        }
        return getSelectResultFromSet(selectResult,overlap);
    }

    public SelectResult orExpression(OrExpression expression,SelectResult selectResult) throws TMDBException, IOException {
        Expression left=expression.getLeftExpression();
        Expression right=expression.getRightExpression();
        SelectResult selectResult1=execute(left,selectResult);
        SelectResult selectResult2=execute(right,selectResult);
        HashSet<Tuple> selectResultSet1=getTupleSet(selectResult1);
        HashSet<Tuple> selectResultSet2=getTupleSet(selectResult2);
        //将selectResultSet2中tuple加入selectResultSet1中，这里将selectResultSet1作为结果集合
        for(Tuple tuple:selectResultSet2){
            selectResultSet1.add(tuple);
        }
        return getSelectResultFromSet(selectResult,selectResultSet1);
    }

    public SelectResult inExpression(InExpression expression, SelectResult selectResult) throws TMDBException, IOException {
        ArrayList<Object> left=formula.formulaExecute(expression.getLeftExpression(),selectResult);
        List<Object> right=new ArrayList<>();
        //in表达式右边可能是一个list
        if(expression.getRightItemsList()!=null){
            for(Expression expression1:((ExpressionList)expression.getRightItemsList()).getExpressions()){
                ArrayList<Object> temp2=formula.formulaExecute(expression1,selectResult);
                right.add(transType(temp2.get(0)));
            }
        }
        //in表达式的右边可能是一个SubSelect
        else if(expression.getRightExpression().getClass().getSimpleName().equals("SubSelect")){
            Select select=new SelectImpl();
            SelectResult temp=select.select(expression.getRightExpression());
            for(int i=0;i<temp.getTpl().tuplelist.size();i++){
                right.add(transType(temp.getTpl().tuplelist.get(i).tuple[0]));
            }
        }
        ArrayList<Tuple> resTuple=new ArrayList<>();
        //最后，如果left存在于right的集合中，就加入到结果集合
        for(int i=0;i<left.size();i++){
            if(right.contains(transType(left.get(i)))) resTuple.add(selectResult.getTpl().tuplelist.get(i));
        }
        selectResult.getTpl().tuplelist=resTuple;
        return selectResult;
    }

    public SelectResult equalsToExpression(EqualsTo expression,SelectResult selectResult) throws TMDBException {
        ArrayList<Object> left=formula.formulaExecute(expression.getLeftExpression(),selectResult);
        ArrayList<Object> right=formula.formulaExecute(expression.getRightExpression(),selectResult);
        HashSet<Tuple> set=new HashSet<>();
        for(int i=0;i<left.size();i++){
            String tempLeft=transType(left.get(i));
            String tempRight=transType(right.get(i));
            //左边和右边相等则加入结果集合。
            if(tempLeft.equals(tempRight)) set.add(selectResult.getTpl().tuplelist.get(i));
        }
        return getSelectResultFromSet(selectResult,set);
    }

    public SelectResult minorThan(MinorThan expression,SelectResult selectResult) throws TMDBException {
        ArrayList<Object> left=formula.formulaExecute(expression.getLeftExpression(),selectResult);
        ArrayList<Object> right=formula.formulaExecute(expression.getRightExpression(),selectResult);
        HashSet<Tuple> set=new HashSet<>();
        for(int i=0;i<left.size();i++){
            String tempLeft=transType(left.get(i));
            String tempRight=transType(right.get(i));
            //左边小于右边，则加入结果集合
            if(tempLeft.compareTo(tempRight)<0) set.add(selectResult.getTpl().tuplelist.get(i));
        }
        return getSelectResultFromSet(selectResult,set);
    }

    public SelectResult greaterThan(GreaterThan expression, SelectResult selectResult) throws TMDBException {
        // 获取表达式左右数据列表
        ArrayList<Object> left = formula.formulaExecute(expression.getLeftExpression(), selectResult);
        ArrayList<Object> right = formula.formulaExecute(expression.getRightExpression(), selectResult);
        HashSet<Tuple> set = new HashSet<>();
        for (int i = 0; i < left.size(); i++){
            String tempLeft = transType(left.get(i));
            String tempRight = transType(right.get(i));
            // 左边大于右边，则加入结果集合
            if (tempLeft.compareTo(tempRight) > 0) {
                set.add(selectResult.getTpl().tuplelist.get(i));
            }
        }
        return getSelectResultFromSet(selectResult, set);
    }

    public HashSet<Tuple> getTupleSet(SelectResult selectResult){
        HashSet<Tuple> set=new HashSet<>();
        for(Tuple tuple:selectResult.getTpl().tuplelist){
            set.add(tuple);
        }
        return set;
    }

    public SelectResult getSelectResultFromSet(SelectResult selectResult, HashSet<Tuple> set){
        TupleList tupleList = new TupleList();
        for(Tuple tuple : set){
            tupleList.addTuple(tuple);
        }
        selectResult.setTpl(tupleList);
        return selectResult;
    }

    // 进行类型转换，很多时候需要使用
    public String transType(Object obj){
        switch (obj.getClass().getSimpleName()) {
            case "String":
                boolean flag = false;
                try{
                    Double temp=Double.parseDouble(String.valueOf(obj));
                    flag=true;
                }
                catch(Throwable throwable){}
                if(flag==true) return String.valueOf(Double.parseDouble(String.valueOf(obj)));
                else return (String)obj;
            case "Float": return String.valueOf((double) obj);
            case "Double": return String.valueOf(obj);
            case "Integer": return String.valueOf((double) obj);
            case "Long": return String.valueOf((double) obj);
            case "Character": return String.valueOf(obj);
            case "Short": return String.valueOf((double) obj);
            default: return "";
        }
    }

}
