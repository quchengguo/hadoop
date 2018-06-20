package mr;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Created by quchengguo on 2018/6/19.
 */
@Slf4j
public class HDFSClient {
    public static void main(String[] args)throws Exception{
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://node1:9000");
        System.setProperty("HADOOP_USER_NAME", "root");
        FileSystem fs = FileSystem.get(conf);
        fs.copyToLocalFile(false, new Path("/myDir/input/my.txt"), new Path("E:\\1.txt") ,true);
        log.info("拷贝文件成功");
        fs.close();
    }
}