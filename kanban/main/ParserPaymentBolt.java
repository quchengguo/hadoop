package com.study;

import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by cheng on 2018/7/3.
 * 获取kafkaspout发送过来的数据
 * 输入：json
 * 输出：javaBean
 */
@Slf4j
public class ParserPaymentBolt  extends BaseRichBolt{
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple inputTuple) {
        log.info("-----------------------");
        long startTime = System.currentTimeMillis();
        String json = inputTuple.getStringByField("value");
        Gson gson = new Gson();
        Payment payment = gson.fromJson(json, Payment.class);
        // 这里发送数据时候，通过declareOutputFields方法传递给下游
        collector.emit(new Values(payment));
        log.info("花费时间:"+(System.currentTimeMillis() - startTime));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("javaBean"));
    }
}
