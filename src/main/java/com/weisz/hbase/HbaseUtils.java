package com.weisz.hbase;

import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;  
import org.apache.hadoop.hbase.filter.FilterList;  
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;  
import org.apache.hadoop.hbase.util.Bytes; 
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.alibaba.fastjson.JSONObject;

import com.weisz.hbase.annotation.HBaseQualifier;
import com.weisz.hbase.annotation.HBaseFamily;
import com.weisz.hbase.annotation.HBaseTable;
import com.weisz.hbase.bean.BaseTable;
import com.weisz.hbase.bean.Condition;

/**
 * hbase操作工具类
 * @author shengzhi.wei
 *
 */
public class HbaseUtils {
	
	static String MASTER = "192.168.163.157:60000";
	static String ZOOKEEPER_QUORUM = "192.168.163.157";
    static String ZOOKEEPER_CLIENT_PORT = "2181";
    
    public HbaseUtils(){}
    public HbaseUtils(String master,String quorum,String clientPort){
    	MASTER = master;
    	ZOOKEEPER_QUORUM = quorum;
    	ZOOKEEPER_CLIENT_PORT = clientPort;
    }
    
    /**
     * 日志
     */
    private static final Logger logger = LoggerFactory.getLogger(HbaseUtils.class);
    
    /**
     * hbase配置
     */
    private static Configuration config = null;
    
    /**
     * hbase连接
     */
    private static Connection connection = null;

    /**
     * hbase操作对象
     */
    private static Admin admin;
  
    /**
     * 初始化admin
     */
    static {
        try {
            config = HBaseConfiguration.create();
            config.set("hbase.zookeeper.property.clientPort", ZOOKEEPER_CLIENT_PORT);
            config.set("hbase.zookeeper.quorum", ZOOKEEPER_QUORUM);
            config.set("hbase.master", MASTER);
            connection = ConnectionFactory.createConnection(config);
            admin = connection.getAdmin();
        } catch (Exception e) {
        	logger.error(e.getMessage());
        }
    }

    
    /**
     * 创建表
     * @param clazz
     * @param isDelete
     * @return
     */
    public static boolean createTable(Class<?> clazz, boolean isDelete) {
        try {  
        	String tableName = clazz.getAnnotation(HBaseTable.class).value();
        	TableName tn = TableName.valueOf(tableName);
            if (admin.tableExists(tn)) {
            	if(!isDelete){
            		logger.info("表["+tableName+"]已经存在,不删除");
            		return true;
            	}
            	admin.disableTable(tn);  
            	admin.deleteTable(tn);  
            }  
            HTableDescriptor table = new HTableDescriptor(tn); 
			if (clazz.isAnnotationPresent(HBaseFamily.class)) {
				String[] familys = clazz.getAnnotation(HBaseFamily.class).value();
				for (String family : familys) {
					HColumnDescriptor hcd = new HColumnDescriptor(family);
					table.addFamily(hcd);
					//table.addFamily(hcd.setCompressionType(Algorithm.GZ)
					//.setCompactionCompressionType(Algorithm.GZ));//列族压缩格式
				}
			}
            admin.createTable(table);  
            //admin.createTable(table, splitKeys);//自定义region预分区 
            return true;
        } catch (Exception e) {  
        	logger.error(e.getMessage());  
            return false;
        }  
    } 
    
    /**
     * 获取表结构
     * @param tableName
     * @throws Exception
     */
    public static List<String> descTable(String tableName) throws Exception {
    	List<String> ret = new ArrayList<String>();
        HTable table= getHTable(tableName);
        HTableDescriptor desc =table.getTableDescriptor();
        HColumnDescriptor[] columnFamilies = desc.getColumnFamilies();
        for(HColumnDescriptor family : columnFamilies){
        	ret.add(Bytes.toString(family.getName()));
        }
        return ret;
    }
    
    
    /**
     * 删除表
     * @param clazz
     * @return
     * @throws Exception
     */
    public static boolean dropTable(Class<?> clazz) {  
    	try {
    		String tableName = clazz.getAnnotation(HBaseTable.class).value();
        	TableName tn = TableName.valueOf(tableName);
			boolean exists = admin.tableExists(tn);
			if (exists) {
				admin.disableTable(tn);  
				admin.deleteTable(tn);  
			}
			return true;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}  
    }
    
    /**
     * 清空表
     * @param tableName
     * @return
     */
    public static boolean truncateTable(String tableName) {
        try {
			// 取得目标数据表的表名对象
			TableName tn = TableName.valueOf(tableName);
			// 设置表状态为无效
			admin.disableTable(tn);
			// 清空指定表的数据
			admin.truncateTable(tn, true);
			return true;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
    }
    
	
    /**
     * 新增列族
     * @param tableName
     * @param familyNames
     * @throws IOException
     */
    public void addColumnFamily(String tableName,String... familyNames) throws IOException{
        // 取得目标数据表的表名对象
        TableName tn = TableName.valueOf(tableName);
        // 创建列族对象
        for (String familyName : familyNames) {
        	HColumnDescriptor hcd = new HColumnDescriptor(familyName);
            // 将新创建的列族添加到指定的数据表
            admin.addColumn(tn, hcd);
		}
    }
    

   /**
    * 删除列族
    * @param tableName
    * @param familyNames
    * @throws IOException
    */
    public void deleteFamily(String tableName,String... familyNames) throws IOException{
        // 取得目标数据表的表名对象
        TableName tn = TableName.valueOf(tableName);

        // 删除指定数据表中的指定列族
        for (String familyName : familyNames) {
        	admin.deleteColumn(tn, Bytes.toBytes(familyName));
		}
    }
    
    
    /** 
     * 插入数据[单条] 
     * @throws Exception 
     */  
    public static boolean add(BaseTable obj) throws Exception {  
    	
        try {  
        	Class<?> clazz = obj.getClass(); 
        	String tableName = clazz.getAnnotation(HBaseTable.class).value();
	        HTable table = getHTable(tableName);  
	        Put put = new Put(Bytes.toBytes(obj.getRowKey()));
	        
	        Field[] fields = clazz.getDeclaredFields();
	        for (Field field : fields) {
	        	if(field.isAnnotationPresent(HBaseQualifier.class)){
            		String qualifier = field.getAnnotation(HBaseQualifier.class).value();
            		String family = qualifier.substring(0, qualifier.indexOf("."));
            		PropertyDescriptor pd = new PropertyDescriptor(field.getName(), clazz);
    		        Method read = pd.getReadMethod();//获得读方法
    		        Object value = read.invoke(obj);
    		        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier),Bytes.toBytes(String.valueOf(value)));
            	}
			}    
            table.put(put);
            return true;
        } catch (IOException e) {  
        	logger.error(e.getMessage());
            return false;
        }  
    }
    
    
    /**
     * 添加数据[批量]
     * @param list
     * @throws Exception
     */
    public static boolean addBatch(List<? extends BaseTable> list) throws Exception {
        try {
        	Class<?> clazz = list.get(0).getClass(); 
        	String tableName = clazz.getAnnotation(HBaseTable.class).value();
            HTable htable = getHTable(tableName); 
            // 这里设置了false,setWriteBufferSize方法才能生效
            htable.setAutoFlushTo(false);
            htable.setWriteBufferSize(5 * 1024 * 1024);
            
            List<Put> puts = new ArrayList<Put>();
            Put put = null;
            for (BaseTable obj : list) {
            	put = new Put(Bytes.toBytes(obj.getRowKey()));
                Field[] fields = clazz.getDeclaredFields();
                for (Field field : fields) {
                	if(field.isAnnotationPresent(HBaseQualifier.class)){
                		String qualifier = field.getAnnotation(HBaseQualifier.class).value();
                		String family = qualifier.substring(0, qualifier.indexOf("."));
                		PropertyDescriptor pd = new PropertyDescriptor(field.getName(), clazz);
        		        Method read = pd.getReadMethod();//获得读方法
        		        Object value = read.invoke(obj);
        		        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier),Bytes.toBytes(String.valueOf(value)));
                	}
        		} 
                puts.add(put);
    		}
            // 添加
            htable.put(puts);
            // 执行
            htable.flushCommits();
            return true;
        }catch (Exception e) {
        	logger.error(e.getMessage());
        	return false;
		}finally {
            // 关闭连接
        }
    }

    
    /**
     * 删除单条数据
     * 
     * @param tablename
     * @param row
     * @throws IOException
     */
    public static boolean delete(Class<?> clazz, String row) throws IOException {
        // 获取htable操作对象
    	String tableName = clazz.getAnnotation(HBaseTable.class).value();
        HTable table = getHTable(tableName); 
        
        if (null==table) {
            return false;
        }
        
        if(StringUtils.isBlank(row)){
        	return false;
        }
        
        try {
            // 创建删除对象
            Delete d = new Delete(Bytes.toBytes(row));
            // 执行删除操作
            table.delete(d);
            return true;
        }catch (Exception e) {
        	logger.error(e.getMessage());
        	return false;
		}finally {
            // 关闭连接
            closeConnect(null, null, null, null, table);
        }
    }

    /**
     * 删除多行数据
     * 
     * @param tablename
     * @param rows
     * @throws IOException
     */
    public static boolean delete(Class<?> clazz, String[] rows) throws IOException {

        // 获取htable操作对象
    	String tableName = clazz.getAnnotation(HBaseTable.class).value();
        HTable table = getHTable(tableName); 
        if(null == table){
        	logger.error("表["+table+"]不存在");
        	return false;
        }
        
        if(null == rows || rows.length == 0){
        	return false;
        }
        
        try {
            // 存储删除对象List
            List<Delete> list = new ArrayList<Delete>();
            for (String row : rows) {
                // 创建删除对象
                Delete d = new Delete(Bytes.toBytes(row));
                // 添加到删除对象List中
                list.add(d);
            }
            table.delete(list);
            return true;
        }catch (Exception e) {
        	logger.error(e.getMessage());
        	return false;
		}finally {
            // 关闭连接
        }
    }
    
    /**
     * 删除列
     * @param tableName
     * @param rowKey
     * @param columnName
     * @throws IOException
     */
    public static void deleteColumns(String tableName, String rowKey, String columnName) throws IOException {  
    	HTable table = getHTable(tableName); 
        Delete delete = new Delete(Bytes.toBytes(rowKey));  
        String familyName = columnName.substring(0,columnName.indexOf("."));
        delete.addColumns(Bytes.toBytes(familyName), Bytes.toBytes(columnName));  
        table.delete(delete); 
    } 
    
    /**
     * 添加列值
     * @param tableName
     * @param rowKey
     * @param columnName
     * @param value
     * @return
     */
    public static boolean addColumn(String tableName, String rowKey, String columnName, String value) {  
    	try {
			HTable table = getHTable(tableName); 
			Put put = new Put(Bytes.toBytes(rowKey));  
			String familyName = columnName.substring(0,columnName.indexOf("."));
			put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName), Bytes.toBytes(value));  
			table.put(put);  
			return true;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}  
    }  

    /**
     * 获取单行数据
     * @param clazz
     * @param row
     * @return
     * @throws Exception
     */
    public static Object getRow(Class<?> clazz, String row) throws Exception {
    	String tableName = clazz.getAnnotation(HBaseTable.class).value();
        HTable table = getHTable(tableName); 
        if(null == table){
        	return null;
        }
        
        try {
            // 创建查询对象
            Get get = new Get(row.getBytes());
            // 查询获得结果
            Result rs = table.get(get);
            BaseTable obj = rs2bean(rs,clazz);
            return obj;
        } catch (IOException e) {
            logger.error("获取失败！", e);
            return null;
        } finally {
            // 关闭连接
            closeConnect(null, null, null, null, table);
        } 
    }

    /**
     * 获取多行数据
     * @param clazz
     * @param rows
     * @return
     * @throws Exception
     */
    public static List<BaseTable> getRows(Class<?> clazz, List<String> rows) throws Exception {
		try {
			// 获取htable操作对象
	    	List<BaseTable> list = null;
	    	String tableName = clazz.getAnnotation(HBaseTable.class).value();
	        HTable table = getHTable(tableName); 
	        if (table == null) {
	        	return null;
	        }
			List<Get> gets = new ArrayList<Get>();// 创建查询操作的List
			for (String row : rows) {
				gets.add(new Get(Bytes.toBytes(row)));
			}
			Result[] results = table.get(gets);// 查询数据
			list = new ArrayList<BaseTable>();
			for (Result rs : results) {
				list.add(rs2bean(rs,clazz));
			}
			return list;
		} catch (IOException e) {
            logger.error("获取数据失败！", e);
        } finally {
            
        }
        return null;
    }
    
    /**
     * 获取特定的列
     * @param clazz
     * @param rowKey
     * @param columnNames
     * @return
     * @throws Exception
     */
    public static Object getColumn(Class<?> clazz, String rowKey,String... columnNames) throws Exception {  
    	String tableName = clazz.getAnnotation(HBaseTable.class).value();
        HTable table = getHTable(tableName); 
        Get get = new Get(Bytes.toBytes(rowKey)); 
        for (String columnName : columnNames) {
        	String familyName = columnName.substring(0,columnName.indexOf("."));
        	get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName)); // 获取指定列族和列修饰符对应的列  
		}
        Result rs = table.get(get);  
        Object obj = rs2bean(rs,clazz);
        System.out.println(JSONObject.toJSONString(obj));
        return obj;
         
    } 
    
    /** 
     * 查询所有数据 
     * @param tableName 
     * @throws Exception 
     */  
    public static List<BaseTable> queryAll(Class<?> clazz) throws Exception {  
        try {  
        	List<BaseTable> list = new ArrayList<BaseTable>();
        	String tableName = clazz.getAnnotation(HBaseTable.class).value();
            HTable table = getHTable(tableName); 
            ResultScanner scanner = table.getScanner(new Scan());
            if(null == scanner){
            	return null;
            }
            for (Result rs : scanner) {
            	BaseTable obj = rs2bean(rs,clazz);
                list.add(obj);
            }  
            return list;
        } catch (IOException e) {  
            return null;
        }  
    } 
    
    
    /**
     * rs转换为bean对象
     * @param rs
     * @param clazz 该类继承BaseTable
     * @return
     * @throws Exception
     */
    public static BaseTable rs2bean(Result rs, Class<?> clazz) throws Exception{
    	BaseTable obj = (BaseTable)clazz.newInstance();
    	obj.setRowKey(new String(rs.getRow()));
        List<Cell> cells = rs.listCells();
        for (Cell cell : cells) {
        	//String family = new String(CellUtil.cloneFamily(cell));
        	String qualifier = new String(CellUtil.cloneQualifier(cell));
        	String value = new String(CellUtil.cloneValue(cell));
        	setValue(obj,qualifier,value);
		}
    	return obj;
    }
    
    /**
     * 给对象成员赋值
     * @param obj 对象
     * @param annotation 属性的注解值[列名]---非变量本身
     * @param value 赋值
     * @throws Exception
     */
    public static void setValue(Object obj, String qualifier, Object value) throws Exception{
    	Class<?> clazz = obj.getClass();
    	Field[] fields = clazz.getDeclaredFields();
        for (Field field : fields) {
        	if(field.isAnnotationPresent(HBaseQualifier.class)){
        		String hbaseQualifier = field.getAnnotation(HBaseQualifier.class).value();
        		if(qualifier.equals(hbaseQualifier)){
        			PropertyDescriptor pd = new PropertyDescriptor(field.getName(), clazz);
    		        Method write = pd.getWriteMethod();
    		        write.invoke(obj, value);
        		}
        	}
		}
    }
    
    
    /**
     *  组合条件查询
     * @param clazz
     * @param condList 条件列表
     */
    public static List<BaseTable> queryByCondition(Class<?> clazz, List<Condition> condList) {  
    	List<BaseTable> ret = new ArrayList<BaseTable>();
        try {  
        	//1、根据类注解获取表名称
        	String tableName = clazz.getAnnotation(HBaseTable.class).value();
            HTable table = getHTable(tableName);
            //过滤器列表
            List<Filter> filters = new ArrayList<Filter>();
            for (Condition cond : condList) {
            	Field field = clazz.getDeclaredField(cond.getFieldName());//根据属性名称获取属性对象
            	if(field.isAnnotationPresent(HBaseQualifier.class)){
            		String qualifier = field.getAnnotation(HBaseQualifier.class).value();//获取属性对象上的注解[列名]
            		String family = qualifier.substring(0,qualifier.indexOf("."));//列族名
                	Filter filter = new SingleColumnValueFilter(Bytes.toBytes(family), Bytes.toBytes(qualifier), cond.getOp(), Bytes.toBytes(cond.getValue()));  
                    filters.add(filter);
            	}
			}
            
            Scan scan = new Scan();  
            scan.setFilter(new FilterList(filters));  
            ResultScanner scanner = table.getScanner(scan);  
            for (Result rs : scanner) {
            	BaseTable obj = rs2bean(rs,clazz);
            	ret.add(obj);
            } 
            scanner.close();  
            return ret;
        } catch (Exception e) {  
            e.printStackTrace();
            return null;
        }  
    } 
    
    /**
     * 根据表名获取表对象
     * @param tableName
     * @return
     * @throws IOException
     */
    public static HTable getHTable(String tableName) throws IOException{
    	 HTable table = (HTable) connection.getTable(TableName.valueOf(tableName)); 
    	 return table;
    }

    /**
     * 关闭连接
     * 
     * @param conn
     * @param mutator
     * @param admin
     * @param htable
     */
    public static void closeConnect(Connection conn, BufferedMutator mutator,
            Admin admin, HTable htable, Table table) {
        if (null != conn) {
            try {
                conn.close();
            } catch (Exception e) {
                logger.error("closeConnect failure !", e);
            }
        }

        if (null != mutator) {
            try {
                mutator.close();
            } catch (Exception e) {
                logger.error("closeBufferedMutator failure !", e);
            }
        }

        if (null != admin) {
            try {
                admin.close();
            } catch (Exception e) {
                logger.error("closeAdmin failure !", e);
            }
        }
        if (null != htable) {
            try {
                htable.close();
            } catch (Exception e) {
                logger.error("closeHtable failure !", e);
            }
        }
        if (null != table) {
            try {
                table.close();
            } catch (Exception e) {
                logger.error("closeTable failure !", e);
            }
        }
    }
}