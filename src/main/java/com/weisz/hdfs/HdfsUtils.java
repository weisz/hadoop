package com.weisz.hdfs;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI; 
  
import org.apache.commons.lang.StringUtils;  
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;  
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.io.IOUtils;
  
/**
 * hdfs工具类
 * @author weisz
 *
 */
public class HdfsUtils {  
  
    public static String HDFSUri = "hdfs://192.168.163.157:9000";
    
    public static FileSystem fs = null; //文件系统
    static{
    	Configuration conf = new Configuration();
        if(StringUtils.isBlank(HDFSUri)){
            // 返回默认文件系统  如果在 Hadoop集群下运行，使用此种方法可直接获取默认文件系统
            try {
                fs = FileSystem.get(conf);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }else{
            // 返回指定的文件系统,如果在本地测试，需要使用此种方法获取文件系统
            try {
                URI uri = new URI(HDFSUri.trim());
                fs = FileSystem.get(uri,conf);
            } catch (Exception e) {
            	e.printStackTrace();
            }
        }
    }
    
   
    /**
     * 创建文件目录
     * 
     * @param path
     */
    public static boolean mkdir(String path) {
        try {
            // 获取文件系统
            //FileSystem fs = getFileSystem();
            
            String hdfsUri = HDFSUri;
            if(StringUtils.isNotBlank(hdfsUri)){
                path = hdfsUri + path;
            }
            
            // 创建目录
            boolean flag = fs.mkdirs(new Path(path));
            
            //释放资源
            fs.close();
            
            return flag;
        } catch (Exception e) {
        	e.printStackTrace();
        	return false;
        }
    }
  
    /**
     * 删除文件或者文件目录
     * 
     * @param path
     */
    public static boolean rmdir(String path) {
        try {
            // 返回FileSystem对象
            //FileSystem fs = getFileSystem();
            
            String hdfsUri = HDFSUri;
            if(StringUtils.isNotBlank(hdfsUri)){
                path = hdfsUri + path;
            }
            
            // 删除文件或者文件目录  delete(Path f) 此方法已经弃用
            boolean flag = fs.delete(new Path(path),true);
            
            // 释放资源
            fs.close();
            return flag;
        } catch (Exception e) {
        	e.printStackTrace();
        	return false;
        }
    }
  
    /**
     * 根据filter获取目录下的文件
     * 
     * @param path
     * @param pathFilter
     * @return String[]
     */
    public static String[] ListFile(String path,PathFilter pathFilter) {
        String[] files = new String[0];
        
        try {
            // 返回FileSystem对象
            //FileSystem fs = getFileSystem();
            
            String hdfsUri = HDFSUri;
            if(StringUtils.isNotBlank(hdfsUri)){
                path = hdfsUri + path;
            }
            
            FileStatus[] status;
            if(pathFilter != null){
                // 根据filter列出目录内容
                status = fs.listStatus(new Path(path),pathFilter);
            }else{
                // 列出目录内容
                status = fs.listStatus(new Path(path));
            }
            
            // 获取目录下的所有文件路径
            Path[] listedPaths = FileUtil.stat2Paths(status);
            // 转换String[]
            if (listedPaths != null && listedPaths.length > 0){
                files = new String[listedPaths.length];
                for (int i = 0; i < files.length; i++){
                    files[i] = listedPaths[i].toString();
                }
            }
            // 释放资源
            fs.close();
        } catch (Exception e) {
        	e.printStackTrace();
        }
        
        return files;
    }
  
    /**
     * 文件上传至 HDFS
     * 
     * @param delSrc
     * @param overwrite
     * @param srcFile
     * @param destPath
     */
    public static void copyFromLocalFile(boolean delSrc, boolean overwrite,String srcFile,String destPath) {
        // 源文件路径是Linux下的路径，如果在 windows 下测试，需要改写为Windows下的路径，比如D://hadoop/djt/weibo.txt
        Path srcPath = new Path(srcFile);
        
        // 目的路径
        String hdfsUri = HDFSUri;
        if(StringUtils.isNotBlank(hdfsUri)){
            destPath = hdfsUri + destPath;
        }
        Path dstPath = new Path(destPath);
        
        // 实现文件上传
        try {
            // 获取FileSystem对象
            //FileSystem fs = getFileSystem();
            fs.copyFromLocalFile(srcPath, dstPath);
            fs.copyFromLocalFile(delSrc,overwrite,srcPath, dstPath);
            //释放资源
            fs.close();
        } catch (IOException e) {
        	e.printStackTrace();
        }
    }
    
    
    /**
     * 从 HDFS 下载文件
     * 
     * @param srcFile
     * @param destPath
     */
    public static void copyToLocalFile(String srcFile,String destPath) {
        // 源文件路径
        String hdfsUri = HDFSUri;
        if(StringUtils.isNotBlank(hdfsUri)){
            srcFile = hdfsUri + srcFile;
        }
        Path srcPath = new Path(srcFile);
        
        // 目的路径是Linux下的路径，如果在 windows 下测试，需要改写为Windows下的路径，比如D://hadoop/djt/
        Path dstPath = new Path(destPath);
        
        try {
            // 获取FileSystem对象
            //FileSystem fs = getFileSystem();
            // 下载hdfs上的文件
            fs.copyToLocalFile(srcPath, dstPath);
            // 释放资源
            fs.close();
        } catch (IOException e) {
        	e.printStackTrace();
        }
    }
    
    /**
     * 获取 HDFS 集群节点信息
     * 
     * @return DatanodeInfo[]
     */
    public static DatanodeInfo[] getHDFSNodes() {
        // 获取所有节点
        DatanodeInfo[] dataNodeStats = new DatanodeInfo[0];
        
        try {
            // 返回FileSystem对象
            //FileSystem fs = getFileSystem();
            
            // 获取分布式文件系统
            DistributedFileSystem hdfs = (DistributedFileSystem)fs;
            
            dataNodeStats = hdfs.getDataNodeStats();
        } catch (IOException e) {
        	e.printStackTrace();
        }
        return dataNodeStats;
    }
    
    
    /**
     * 查找某个文件在 HDFS集群的位置
     * 
     * @param filePath
     * @return BlockLocation[]
     */
    public static BlockLocation[] getFileBlockLocations(String filePath) {
        // 文件路径
        String hdfsUri = HDFSUri;
        if(StringUtils.isNotBlank(hdfsUri)){
            filePath = hdfsUri + filePath;
        }
        Path path = new Path(filePath);
        
        // 文件块位置列表
        BlockLocation[] blkLocations = new BlockLocation[0];
        try {
            // 返回FileSystem对象
            //FileSystem fs = getFileSystem();
            // 获取文件目录 
            FileStatus filestatus = fs.getFileStatus(path);
            //获取文件块位置列表
            blkLocations = fs.getFileBlockLocations(filestatus, 0, filestatus.getLen());
        } catch (IOException e) {
        	e.printStackTrace();
        }
        return blkLocations;
    }
    
    
    /**
     * 文件重命名
     * 
     * @param srcPath
     * @param dstPath
     */
    public boolean rename(String srcPath, String dstPath){
        boolean flag = false;
        try    {
            // 返回FileSystem对象
            //FileSystem fs = getFileSystem();
            
            String hdfsUri = HDFSUri;
            if(StringUtils.isNotBlank(hdfsUri)){
                srcPath = hdfsUri + srcPath;
                dstPath = hdfsUri + dstPath;
            }
            
            flag = fs.rename(new Path(srcPath), new Path(dstPath));
        } catch (IOException e) {
        	e.printStackTrace();
        }
        
        return flag;
    }
    
    
    /**
     * 判断目录是否存在
     * 
     * @param srcPath
     * @param dstPath
     */
    public static boolean existDir(String filePath){
        boolean flag = false;
        
        if (StringUtils.isEmpty(filePath)){
            return flag;
        }
        
        try{
            Path path = new Path(filePath);
            // FileSystem对象
            //FileSystem fs = getFileSystem();
            
            if (fs.isDirectory(path)){
                flag = true;
            }
            
            flag = fs.exists(path);
        }catch (Exception e){
        	e.printStackTrace();
        }
        
        return flag;
    }
    
    /**
     * 创建文件,并写入内容
     * @param destFile 目标文件全路径/user/data/test.log
     * @param content 文件内容 Hello World
     */
    public static void create(String destFile, String content){
    	//FileSystem fs = getFileSystem();
    	  
        try {
        	FSDataOutputStream os = fs.create(new Path(destFile));  
            os.write(content.getBytes());  
            os.flush();
			os.close();
		} catch (IOException e) {
			e.printStackTrace();
		}  
    }
    
    
    /**
     * 获取hdfs文件流
     * @param destFile 文件全路径 /user/data/test.log
     * @return
     */
    public static InputStream getFileInputStream(String destFile){
    	//FileSystem fs = getFileSystem();
		try {
			InputStream is = fs.open(new Path(destFile));
			IOUtils.copyBytes(is, System.out, 1024, true);
			return is;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
    }
    
    public static void main(String[] args) {
    	//boolean flag = mkdir("/usr/data/hadoop/2017-07-23");
    	//boolean flag = rmdir("/usr/data");
    	boolean flag = existDir("/user/data/test.log");
    	System.out.println(flag);
    	//getFileInputStream("/user/data/test.log");
	}
    
  
}  