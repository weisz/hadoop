package com.weisz.hbase.bean;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;

public class Condition {
	
	private String fieldName;
	private CompareOp op;
	private String value;
	public String getFieldName() {
		return fieldName;
	}
	public void setFieldName(String fieldName) {
		this.fieldName = fieldName;
	}
	public CompareOp getOp() {
		return op;
	}
	public void setOp(CompareOp op) {
		this.op = op;
	}
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
	
}
