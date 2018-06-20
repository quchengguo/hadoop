package mr;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by quchengguo on 2018/6/19.
 * KEYIN:表示数据输入时候数据类型，默认叫InputFormat，它的行为一行一行的读取，读取一行返回一行给mr，keyin表示每一行的偏移量，
 *          因此类型为long
 * VALUEIN:表述数据输入时候value的类型，在默认情况下valuein就是这一行的内容，因此类型是string
 * ---
 * KEYOUT:表示mapper数据输出时候key的数据类型 在本案例中输出key是单词 因此数据类型是String
 *VALUEOUT:表示mapper数据输出时候value的数据类型 在本案例中输出是key单词的次数，因此数据类型是Integer
 */
@Slf4j
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    /**
     * 业务逻辑实现的方法 每传入一个《key,value》该方法就被调用一次
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1.拿到传入进来的一行内容，转换为string
        String line = value.toString();
        // 2.将该string按照分割符进行一行内容的切割
        String[] words = line.split(" ");
        // 3.遍历数组，每出现一个单词 就标记一个数字1《单词，次数》
        for(String word : words){
            // 使用mr程序的上下文，输入输出通过上下文衔接
            context.write(new Text(word),new IntWritable(1));
        }
    }
}
