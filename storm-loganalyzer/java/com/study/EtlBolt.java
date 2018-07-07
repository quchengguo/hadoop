package com.study;

import org.apache.storm.topology.*;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * Created by quchengguo on 2018/7/7.
 * 做ETL工作  抽取、转换、加载
 *
 */
public class EtlBolt extends BaseBasicBolt {
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String data = tuple.getStringByField("data");
        String[] fields = data.split("\t");
        if(fields.length == 13){
            Message message = new Message(fields);
            basicOutputCollector.emit(new Values(message));
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("message"));
    }
}
