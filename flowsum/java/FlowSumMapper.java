package flowsum;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by quchengguo on 2018/6/20.
 * 分
 *需求：统计每个手机号的总流量
 */
@Slf4j
public class FlowSumMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    Text k = new Text();
    FlowBean v = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 按照空格分割，手机号作为key，上下行流量作为value
        String line = value.toString();
        log.info("******" + line);
        String[] fields = line.split("\t");
        log.info("fieldsSize:{}", fields.length);
        String phoneNum = fields[1];
        Long upFlow = Long.parseLong(fields[fields.length - 3]);
        Long downFlow = Long.parseLong(fields[fields.length - 2]);
        k.set(phoneNum);
        v.set(upFlow,downFlow);
        context.write(k, v);
    }
}
