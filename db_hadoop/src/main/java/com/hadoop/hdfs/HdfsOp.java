package com.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.LoggerFactory;


import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * @author LR
 * @create 2021-05-07:20:01
 *
 * Java代码操作HDFS
 */
public class HdfsOp {
    public static void main(String[] args) throws Exception{

        // 创建一个对象
        Configuration conf = new Configuration();
        // 指定HDFS的地址
        conf.set("fs.defaultFS", "hdfs://192.168.142.100:9000");
        // 获取操作HDFS对象
        FileSystem fileSystem = FileSystem.get(conf);

        // 上传文件
        put(fileSystem);
        // 下载文件
//        get(fileSystem);
        //  删除文件
//        delete(fileSystem);

    }

    private static void delete(FileSystem fileSystem) throws IOException {
        // 删除文件
        boolean flag = fileSystem.delete(new Path("/hello.txt"));
        if (flag){
            System.out.println("success");
        }else{
            System.out.println("fail");
        }
    }

    private static void get(FileSystem fileSystem) throws IOException {
        // 下载文件
        //获取HDFS文件系统的输入流
        FSDataInputStream fis = fileSystem.open(new Path("/user.txt"));
        // 获取本地文件的输出流
        FileOutputStream fos = new FileOutputStream("D:\\code\\JAVA\\bigdata\\user.txt");
        IOUtils.copyBytes(fis, fos, 1024, true);
    }

    private static void put(FileSystem fileSystem) throws IOException {
        // 上传文件
        // 获取本地文件的输入流
        FileInputStream fis = new FileInputStream("D:\\code\\JAVA\\bigdata\\hello.txt");
        // 获取HDFS文件系统的输出流
        FSDataOutputStream fos = fileSystem.create(new Path("/hello.txt"));
        // 通过工具类把输入流拷贝到输出流中去
        IOUtils.copyBytes(fis, fos, 1024, true);
    }
}
