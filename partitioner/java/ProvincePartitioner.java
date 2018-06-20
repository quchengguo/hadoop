package partitioner;

import flowsum.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

/**
 * Created by quchengguo on 2018/6/20.
 * 需求：根据归属地输出流量统计数据结果到不同文件
 */
public class ProvincePartitioner extends Partitioner<Text, FlowBean>{
    static HashMap<String, Integer> provinceMap = new HashMap<>();

    static {
        provinceMap.put("135", 0);
        provinceMap.put("136", 1);
        provinceMap.put("137", 2);
        provinceMap.put("138", 3);
        provinceMap.put("139", 4);
    }

    @Override
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
        Integer code = provinceMap.get(text.toString().substring(0,3));
        return code == null ? 5 : code;
    }
}
