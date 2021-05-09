package com.hadoop.mr;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.File;


/**
 * 小文件解决方案之SequenceFile
 * @author LR
 * @create 2021-05-09:15:03
 */
public class SmallFileSeq {

    public static void main(String[] args) throws Exception{
        //生成SequenceFile大文件
        write("D:\\BaiduNetdiskDownload\\大数据\\git-master\\bigdata_course_materials\\hadoop\\mapreduce+yarn\\smallFile", "/seqFile");
        //读取SequenceFile大文件
        read("/seqFile");
    }

    public static void write(String inputDir, String outputFile) throws Exception{

        // 创建一个对象
        Configuration conf = new Configuration();
        // 指定HDFS的地址
        conf.set("fs.defaultFS", "hdfs://192.168.142.100:9000");

        // 获取HDFS的对象
        FileSystem fileSystem = FileSystem.get(conf);
        // 删除HDFS的对象
        fileSystem.delete(new Path(outputFile));

        SequenceFile.Writer.Option[] opts = new SequenceFile.Writer.Option[]{
                // 输出路径
                SequenceFile.Writer.file(new Path(outputFile)),
                SequenceFile.Writer.keyClass(Text.class),
                SequenceFile.Writer.valueClass(Text.class)
        };

        // 创建一个writer实例
        SequenceFile.Writer writer = SequenceFile.createWriter(conf, opts);
        // 指定需要亚索的文件目录
        File inputDirPath = new File(inputDir);
        if (inputDirPath.isDirectory()){
            // 获取文件的目录
            File[] files = inputDirPath.listFiles();
            for (File file: files) {
                // 获取文件中的所有内容
                String content = FileUtils.readFileToString(file, "UTF-8");
                // 获取文件名
                String fileName = file.getName();

                Text key =  new Text(fileName);
                Text value = new Text(content);
                writer.append(key, value);
            }
        }
        writer.close();

    }

    public static void read(String inputFile) throws Exception{
        // 创建一个对象
        Configuration conf = new Configuration();
        // 指定HDFS的地址
        conf.set("fs.defaultFS", "hdfs://192.168.142.100:9000");

        // 创建一个阅读器
        SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(new Path(inputFile)));

        Text key = new Text();
        Text value = new Text();
        // 循环读取数据
        while (reader.next(key, value)){
            // 输出文件名称
            System.out.print("文件名" + key.toString() + ",");
            // 输出文件内容
            System.out.println("文件内容" + value.toString() + ",");
        }
        reader.close();
    }
}
