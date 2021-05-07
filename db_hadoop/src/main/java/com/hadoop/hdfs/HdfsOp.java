package com.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;


import java.io.FileInputStream;
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

        extracted(fileSystem);

    }

    private static void put(FileSystem fileSystem) throws IOException {
        // 上传文件
        // 获取本地文件的输入流
        FileInputStream fis = new FileInputStream("D:\\code\\JAVA\\bigdata\\user.txt");
        // 获取HDFS文件系统的输出流
        FSDataOutputStream fos = fileSystem.create(new Path("/user.txt"));
        // 通过工具类把输入流拷贝到输出流中去
        IOUtils.copyBytes(fis, fos, 1024, true);
    }
}
