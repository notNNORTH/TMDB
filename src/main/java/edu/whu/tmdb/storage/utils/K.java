package edu.whu.tmdb.storage.utils;

import java.io.Serializable;
import java.util.Objects;

public class K implements Serializable, Comparable{

    public String key = "";


    public K(){
    }

    public K(String key){
        this.key = key;
    }

    public K(byte[] bytes){
        this.key = new String(bytes);
    }

    public byte[] serialize(){
        return this.key.getBytes();
    }

    @Override
    public int compareTo(Object o) {
        if(o == null)
            return 0;
        if(o instanceof K){
            return this.key.compareTo(((K) o).key);
        }
        return 0;
    }

    @Override
    public String toString(){
        return this.key;
    }

    @Override
    public boolean equals(Object obj){
        // 如果是同一个对象，直接返回true
        if (this == obj) {
            return true;
        }

        // 如果obj为null或者不是同一个类的实例，返回false
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        // 自定义相等性比较规则: 比较string
        return this.key.equals(((K) obj).key);
    }

    @Override
    public int hashCode(){
        return this.key.hashCode();
    }


}
