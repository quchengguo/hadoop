package flowsum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by quchengguo on 2018/6/20.
 * 主类
 */
public class FlowSumDriver {
    public static void main(String[] args) throws Exception{
        // 1.Job封装mr相关信息
        Job job = Job.getInstance(new Configuration());
        // 2.指定运行主类
        job.setJarByClass(FlowSumDriver.class);
        // 3.指定mapper
        job.setMapperClass(FlowSumMapper.class);
        // 4.指定reduce
        job.setReducerClass(FlowSumReducer.class);
        // 5.指定mapper 的 k v
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        // 6.指定reduce k v
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        // 7.指定本次输入数据路径和最终输出结果路径
        FileInputFormat.setInputPaths(job, "E:\\hadooptemp\\input\\flowsum");
//        FileInputFormat.setInputPaths(job, args[0]);
        // 一开始output路径不能存在
        FileOutputFormat.setOutputPath(job, new Path("E:\\hadooptemp\\output\\flowsum"));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // 8.提交程序，打印执行情况
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);

    }
}
