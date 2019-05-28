package demo;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;

import com.alibaba.fastjson.JSONObject;
import com.weisz.hbase.HbaseUtils;
import com.weisz.hbase.bean.BaseTable;
import com.weisz.hbase.bean.Condition;
import com.weisz.hbase.bean.UserTable;

public class HbaseTest {

	public static void main(String[] args) throws Exception {
    	List<UserTable> userList = new ArrayList<UserTable>();
    	UserTable user = new UserTable();
    	user.setRowKey("20170805001");
    	user.setNickName("shengzhi.wei");
    	user.setFirstName("韦");
    	user.setPassword("123456");
    	userList.add(user);
    
    	user = new UserTable();
    	user.setRowKey("20170805002");
    	user.setNickName("kunyan.lai");
    	user.setFirstName("赖");
    	user.setPassword("000000");
    	userList.add(user);
    	
    	user = new UserTable();
    	user.setRowKey("20170805003");
    	user.setNickName("fanpeng.pei");
    	user.setFirstName("裴");
    	user.setPassword("999999");
    	boolean flag = HbaseUtils.createTable(UserTable.class,true);
    	System.out.println("createTable:"+flag);
    	flag = HbaseUtils.add(user);
    	System.out.println("add:"+flag);
    	flag = HbaseUtils.addBatch(userList);
    	System.out.println("addBatch:"+flag);
    	List<BaseTable> all = HbaseUtils.queryAll(UserTable.class);
    	for (BaseTable baseTable : all) {
			System.out.println(JSONObject.toJSONString(baseTable));
		}
    	
    	System.out.println("----ss-------@@------------###----");
    	List<Condition> list = new ArrayList<Condition>();
    	Condition cond = new Condition();
    	cond.setFieldName("nickName");
    	cond.setOp(CompareOp.EQUAL);
    	cond.setValue("kunyan.lai");
    	list.add(cond);
    	List<BaseTable> ll = HbaseUtils.queryByCondition(UserTable.class,list);
    	for (BaseTable baseTable : ll) {
			System.out.println(JSONObject.toJSONString(baseTable));
		}
		
//		HbaseUtils.getColumn(UserTable.class, "201708011630250001", "name.firstName","name.nickName");
//		HbaseUtils.deleteColumns("sys_user", "201708011630250003", "password.password");
//		HbaseUtils.addColumn("sys_user", "201708011630250003", "password.password", "5555");
//		Object obj = HbaseUtils.getRow(UserTable.class, "201708011630250003");
//		System.out.println(JSONObject.toJSONString(obj));
//		HbaseUtils.descTable("sys_user");
    }

}
