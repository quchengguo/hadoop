package mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by quchengguo on 2018/6/19.
 * 组装程序运行时所需要的信息
 * 运行该程序有两种方式：
 * 1.打包发布到linux中使用yarn jar xxx.jar 输入目录 输出目录
 * 2.直接在本地运行，前提是windows中得有hadoop环境
 *
 */
public class WordCountDriver {
    public static void main(String[] args) throws Exception{
        // 1.Job封装mr相关信息
        Job job = Job.getInstance(new Configuration());
        // 2.指定运行主类
        job.setJarByClass(WordCountDriver.class);
        // 3.指定mapper
        job.setMapperClass(WordCountMapper.class);
        // 4.指定reduce
        job.setReducerClass(WordCountReducer.class);
        // 5.指定mapper 的 k v
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // 6.指定reduce k v
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // 7.指定本次输入数据路径和最终输出结果路径
        FileInputFormat.setInputPaths(job, "hdfs://node1:9000/wordcount/input/input.txt");
//        FileInputFormat.setInputPaths(job, args[0]);
        // 一开始output路径不能存在
        FileOutputFormat.setOutputPath(job, new Path("E:\\output"));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // 8.提交程序，打印执行情况
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }
}