package com.study;

import org.apache.storm.shade.com.codahale.metrics.MetricRegistryListener;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by quchengguo on 2018/7/7.
 * <p>
 * 模拟类，生成log日志。
 * 正常情况下使用KafkaSpout去消费kafka中的消息
 */
public class MySpout extends BaseRichSpout {
    private SpoutOutputCollector collector;

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    public void nextTuple() {
        // 发一条伪造的数据
        String data = "http://www.itcast.cn/\thttp://www.itcast.cn/product?id=1002\t111\t1\t30\t192.168.114.123\tsid12345\tzhangsan\th|keycount|head|category_02a\twin\tchrome\t1366*768\t123*345";
        collector.emit(new Values(data));
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("data"));
    }
}
