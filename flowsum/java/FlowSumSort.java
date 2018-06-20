package flowsum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by quchengguo on 2018/6/20.
 */
public class FlowSumSort {
    public static class FlowSumSortMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

        Text v = new Text();
        FlowBean k = new FlowBean();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            String[] fields = line.split("\t");

            String phoneNum = fields[0];

            long upFlow = Long.parseLong(fields[1]);

            long downFlow = Long.parseLong(fields[2]);

            k.set(upFlow, downFlow);
            v.set(phoneNum);

            context.write(k, v);

        }

    }

    public static class FlowSumSortReducer extends Reducer<FlowBean, Text, Text, FlowBean> {

        @Override
        protected void reduce(FlowBean key, Iterable<Text> valus, Context context)
                throws IOException, InterruptedException {

            context.write(valus.iterator().next(), key);
        }
    }

    public static void main(String[] args) throws Exception {


        Job job = Job.getInstance(new Configuration());

        //指定我这个 job 所在的 jar包位置
        job.setJarByClass(FlowSumSort.class);

        //指定我们使用的Mapper是那个类  reducer是哪个类
        job.setMapperClass(FlowSumSortMapper.class);
        job.setReducerClass(FlowSumSortReducer.class);

        // 设置我们的业务逻辑 Mapper 类的输出 key 和 value 的数据类型
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        // 设置我们的业务逻辑 Reducer 类的输出 key 和 value 的数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        FileInputFormat.setInputPaths(job, new Path("E:\\hadooptemp\\output\\flowsum"));
        // 指定处理完成之后的结果所保存的位置
        FileOutputFormat.setOutputPath(job, new Path("E:\\hadooptemp\\output\\outputsort"));

        // 向 yarn 集群提交这个 job
        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);

    }

}
