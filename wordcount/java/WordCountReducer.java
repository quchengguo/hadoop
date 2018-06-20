package mr;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by quchengguo on 2018/6/19.
 *
 * KEYOUT:单词
 * VALUEOUT:单词出现的总次数
 */
@Slf4j
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    /**
     * reduce阶段具体业务类的实现方法
     * 按照key是否相同作为一组去调用reduce方法
     * hadoop [1,1]
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for(IntWritable value : values){
            count += value.get();
        }
        context.write(key, new IntWritable(count));
    }
}
