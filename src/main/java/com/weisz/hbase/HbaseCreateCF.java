package com.weisz.hbase;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;


/**
 * hbase创建表封装的ColumnFamily
 * @author shengzhi.wei
 *
 */
public class HbaseCreateCF {

    /**
     * key: ColumnFamily名字
     * value:压缩格式,每个ColumnFamily都可以设置不同的压缩格式，或者只压缩部分ColumnFamily
     */
    private List<Map<String, Compression.Algorithm>> list;

    public List<Map<String, Compression.Algorithm>> getList() {
        return list;
    }

    public void setList(List<Map<String, Compression.Algorithm>> list) {
        this.list = list;
    }

    public HbaseCreateCF(List<Map<String, Algorithm>> list) {
        super();
        this.list = list;
    }

    public HbaseCreateCF() {
        super();
    }

    @Override
    public String toString() {
        return "HbaseCreateCF [list=" + list + "]";
    }

}