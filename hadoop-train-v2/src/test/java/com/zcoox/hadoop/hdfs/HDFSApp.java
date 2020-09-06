package com.zcoox.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

/**
 * HDFS App
 * 使用Java API操作HDFS文件系统
 *
 * 关键点
 * 1）创建 Configuration
 * 2）获取 FileSystem
 * 3）对 HDFS API 的操作
 */
public class HDFSApp {

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop000:8020"), config, "hadoop");
        boolean result = fileSystem.mkdirs(new Path("/hdfsapi/test"));
        System.out.println(result);
    }

}
