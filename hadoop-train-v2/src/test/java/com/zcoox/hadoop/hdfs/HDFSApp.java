package com.zcoox.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.PropertyConfigurator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

/**
 * HDFS App
 * 使用Java API操作HDFS文件系统
 * <p>
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

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private Configuration configuration = null;

    private FileSystem fileSystem = null;

    private static final String HDFS_URI = "hdfs://hadoop000:8020";

    private static final String HDFS_USER = "hadoop";

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        logger.info(">>>>>>>>>>>> init <<<<<<<<<<<<");
        configuration = new Configuration();
        fileSystem = FileSystem.get(new URI(HDFS_URI), configuration, HDFS_USER);
    }

    /**
     * mkdirs
     *
     * @throws IOException
     */
    @Test
    public void mkdirsTest() throws IOException {
        boolean res = fileSystem.mkdirs(new Path("/hdfsapi/newtest5"));
        System.out.println(res);
    }

    @After
    public void destory() {
        configuration = null;
        fileSystem = null;
        logger.info(">>>>>>>>>> destory <<<<<<<<<<<");
    }

}
