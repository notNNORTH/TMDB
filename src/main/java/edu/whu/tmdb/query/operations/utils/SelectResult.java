/**
 * @className: SelectResult
 * @Package: edu.whu.tmdb.query.operations.utils
 * @Description: select的输出结果，对应传统数据库打印的结果，另外此处附带其他属性若干
 * Last modified by lzp, 2023.10.26
 */

package edu.whu.tmdb.query.operations.utils;

import edu.whu.tmdb.storage.memory.TupleList;

public class SelectResult {
    TupleList tpl;          // 元组数据列表
    String[] className;     // 字段所属的类名
    String[] attrname;      // 字段名
    String[] alias;         // 字段的别名，在进行select时会用到
    int[] attrid;           // 显示时使用
    String[] type;          // 字段数据类型(char, int)

    public SelectResult(TupleList tpl, String[] className, String[] attrname, String[] alias, int[] attrid, String[] type) {
        this.tpl = tpl;
        this.className = className;
        this.attrname = attrname;
        this.alias = alias;
        this.attrid = attrid;
        this.type = type;
    }

    public SelectResult(){}

    // 读写元组数据
    public void setTpl(TupleList tpl) { this.tpl = tpl; }

    public TupleList getTpl() { return tpl; }

    // 读写字段所属类名
    public void setClassName(String[] className) { this.className = className; }

    public String[] getClassName() { return className; }

    // 读写字段名
    public void setAttrname(String[] attrname) { this.attrname = attrname; }

    public String[] getAttrname() { return attrname; }

    // 读写字段别名
    public void setAlias(String[] alias) { this.alias = alias; }

    public String[] getAlias() { return alias; }

    // 读写字段id（我也不知道有什么用
    public void setAttrid(int[] attrid) { this.attrid = attrid; }

    public int[] getAttrid() { return attrid; }

    // 读写字段属性
    public void setType(String[] type) { this.type = type; }

    public String[] getType() { return type; }
}
