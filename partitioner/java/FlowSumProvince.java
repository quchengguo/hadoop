package partitioner;

import flowsum.FlowBean;
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
public class FlowSumProvince {
    public static class FlowSumProvinceMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

        Text k = new Text();
        FlowBean v = new FlowBean();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            //拿取一行文本转为String
            String line = value.toString();
            //按照分隔符\t进行分割
            String[] fileds = line.split("\t");
            //获取用户手机号
            String phoneNum = fileds[1];

            long upFlow = Long.parseLong(fileds[fileds.length - 3]);
            long downFlow = Long.parseLong(fileds[fileds.length - 2]);

            k.set(phoneNum);
            v.set(upFlow, downFlow);

            context.write(k, v);

        }
    }

    public static class FlowSumProvinceReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

        FlowBean v = new FlowBean();

        @Override
        protected void reduce(Text key, Iterable<FlowBean> flowBeans, Context context) throws IOException, InterruptedException {

            long upFlowCount = 0;
            long downFlowCount = 0;

            for (FlowBean flowBean : flowBeans) {

                upFlowCount += flowBean.getUpFlow();

                downFlowCount += flowBean.getDownFlow();

            }
            v.set(upFlowCount, downFlowCount);

            context.write(key, v);
        }

    }

    public static void main(String[] args) throws Exception{
        Job job = Job.getInstance(new Configuration());

        //指定我这个 job 所在的 jar包位置
        job.setJarByClass(FlowSumProvince.class);

        //指定我们使用的Mapper是那个类  reducer是哪个类
        job.setMapperClass(FlowSumProvinceMapper.class);
        job.setReducerClass(FlowSumProvinceReducer.class);

        // 设置我们的业务逻辑 Mapper 类的输出 key 和 value 的数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        // 设置我们的业务逻辑 Reducer 类的输出 key 和 value 的数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);


        //这里设置运行reduceTask的个数
        //分区个数 = reducetask个数   正常执行
        //分区个数 < reducetask个数  正常执行   只不过会有空的结果文件产生
        //分区个数 > reducetask个数  错误  Illegal partition
        job.setNumReduceTasks(6);
        //这里指定使用我们自定义的分区组件
        job.setPartitionerClass(ProvincePartitioner.class);
        FileInputFormat.setInputPaths(job, new Path("E:\\hadooptemp\\input\\flowsum"));
        // 指定处理完成之后的结果所保存的位置
        FileOutputFormat.setOutputPath(job, new Path("E:\\hadooptemp\\output\\outputProvince"));
        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);
    }
}
