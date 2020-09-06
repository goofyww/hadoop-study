package com.zcoox.hadoop.hdfs;

import com.alibaba.fastjson.JSONPObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.PropertyConfigurator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mortbay.util.ajax.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
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
        configuration.set("dfs.replication", "1");
        fileSystem = FileSystem.get(new URI(HDFS_URI), configuration, HDFS_USER);
    }

    /**
     * mkdirs
     *
     * @throws IOException
     */
    @Test
    public void mkdirsTest() throws IOException {
        boolean res = fileSystem.mkdirs(new Path("/hdfsapi/newtest/testdir"));
        System.out.println(res);
    }

    /**
     * 查看HDFS内容
     *
     * @throws IOException
     */
    @Test
    public void openTest() throws IOException {
        FSDataInputStream in = fileSystem.open(new Path("/kafka/kafka.txt"));
        IOUtils.copyBytes(in, System.out, 1024);
    }

    /**
     * 创建文件并写入内容
     *
     * @throws IOException
     */
    @Test
    public void createTest() throws IOException {
//        FSDataOutputStream out = fileSystem.create(new Path("/hdfsapi/newtest/a.txt"));
        FSDataOutputStream out = fileSystem.create(new Path("/hdfsapi/newtest/c.txt"));
        String outstr = "hello china-2 \nhello USA \nhello Jepan \n";
        out.write(outstr.getBytes());
        out.flush();
        out.close();
    }

    /**
     * 副本系数深度解析
     * ⚠️ 在服务器中以shell命令行的方式执行api操作是以hadoop配置的为准
     * ⚠️ 在以java作为客户端的方式执行api操作则以jar包中提供的默认配置为准
     * 详见 hadoop-hdfs.jar 下 hdfs.default.xml
     */
    @Test
    public void testReplication() {
        System.out.println(configuration.get("dfs.replication"));
    }

    /**
     * 重命名
     *
     * @throws IOException
     */
    @Test
    public void renameTest() throws IOException {
        Path oldPath = new Path("/hdfsapi/newtest/c.txt");
        Path newPath = new Path("/hdfsapi/newtest/d.txt");
        boolean res = fileSystem.rename(oldPath, newPath);
        System.out.println(res ? "success" : "failure");
    }

    /**
     * copyFromLocalFile 上传
     *
     * @throws IOException
     */
    @Test
    public void copyFromLocalTest() throws IOException {
        Path localPath = new Path("/Users/gf/Desktop/information/SpringCloud");
        Path remotePath = new Path("/hdfsapi/");
        fileSystem.copyFromLocalFile(localPath, remotePath);
    }

    /**
     * 上传大文件（带进度）
     */
    @Test
    public void copyFromLocalLargeTest() throws IOException {
        File file = new File("/Users/gf/Desktop/book/Java核心技术 卷2 高级特性 原书第10版.pdf");
        InputStream in = new BufferedInputStream(new FileInputStream(file));
        final float fileSize = file.length() / 65536;
        FSDataOutputStream out = fileSystem.create(new Path("/hdfsapi/newtest/Java核心技术 卷2 高级特性 原书第10版.pdf"), new Progressable() {
            long fileCount = 0;

            @Override
            public void progress() {
                fileCount++;
                System.out.println("总进度：" + (fileCount / fileSize) * 100 + " %");
            }
        });
        IOUtils.copyBytes(in, out, 4096);
        out.flush();
        out.close();
    }

    /**
     * 拷贝HDFS文件到本地：下载
     */
    @Test
    public void copyToLocalFileTest() throws IOException {
        Path src = new Path("/hdfsapi/newtest/d.txt");
        // TODO 此处是否可以做成多线程
        Path sdt = new Path("/Users/gf/Desktop");
        fileSystem.copyToLocalFile(src, sdt);
    }

    /**
     * 列出目标文件夹下所有文件
     */
    @Test
    public void listStatusTest() throws IOException {
        FileStatus[] statuses = fileSystem.listStatus(new Path("/hdfsapi/newtest/"));
        for (FileStatus file : statuses) {
            String isDir = file.isDirectory() ? "文件夹" : "文件";
            String premission = file.getPermission().toString();
            short replication = file.getReplication();
            long lenth = file.getLen();
            String path = file.getPath().toString();
            System.out.println(isDir + "\t" +
                    premission + "\t" +
                    replication + "\t" +
                    lenth + "\t" +
                    path);
        }
    }

    /**
     * 递归列出目标文件夹下所有文件
     * 仅能列出文件
     */
    @Test
    public void listFilesTest() throws IOException {
        RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(new Path("/hdfsapi/"), true);
        while (iterator.hasNext()) {
            LocatedFileStatus file = iterator.next();
            String isDir = file.isDirectory() ? "文件夹" : "文件";
            String premission = file.getPermission().toString();
            short replication = file.getReplication();
            long lenth = file.getLen();
            String path = file.getPath().toString();
            System.out.println(isDir + "\t" +
                    premission + "\t" +
                    replication + "\t" +
                    lenth + "\t" +
                    path);
        }
    }

    /**
     * 查看文件块信息
     */
    @Test
    public void getFileBlkLocations() throws IOException {
        FileStatus file = fileSystem.getFileStatus(new Path("/hdfsapi/newtest/Java核心技术 卷2 高级特性 原书第10版.pdf"));
        BlockLocation[] blockLocations = fileSystem.getFileBlockLocations(file, 0, file.getLen());
        for (BlockLocation blockLocation : blockLocations) {
            for (String name : blockLocation.getNames()) {
                String[] strings = blockLocation.getHosts();
                System.out.println(name + ":" + blockLocation.getOffset() + ":" + blockLocation.getLength() + ":" + JSON.toString(strings));
            }
        }
    }

    /**
     * 删除文件
     */
    @Test
    public void delTest() throws IOException {
        boolean res = fileSystem.delete(new Path("/hdfsapi/newtest/Java核心技术 卷2 高级特性 原书第10版.pdf"), true);
        System.out.println(res);
    }

    @After
    public void destory() {
        configuration = null;
        fileSystem = null;
        logger.info(">>>>>>>>>> destory <<<<<<<<<<<");
    }

}
