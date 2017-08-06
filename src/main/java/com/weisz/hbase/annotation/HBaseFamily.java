package com.weisz.hbase.annotation;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


/**
 * 列族
 * Family和Qualifier的设置
 * 极端1:每隔
 * 可以每一列都设置成一个family，也可以只有一个family，但所有列都是其中的一个qualifier，那么有什么区别呢？
 * family越多，那么获取每一个cell数据的优势越明显，因为io和网络都减少了，
 * 而如果只有一个family，那么每一次读都会读取当前rowkey的所有数据，网络和io上会有一些损失。
 * 当然如果要获取的是固定的几列数据，那么把这几列写到一个family中比分别设置family要更好，因为只需一次请求就能拿回所有数据
 * @author shengzhi.wei
 *
 */
@Target({ java.lang.annotation.ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface HBaseFamily {
	
	String[] value();
}
