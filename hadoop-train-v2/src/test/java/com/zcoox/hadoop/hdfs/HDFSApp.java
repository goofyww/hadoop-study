package com.zcoox.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

/**
 * HDFS App
 */
public class HDFSApp {

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop000:8020"), config, "hadoop");
        boolean result = fileSystem.mkdirs(new Path("/hdfsapi/test"));
        System.out.println(result);
    }

}
