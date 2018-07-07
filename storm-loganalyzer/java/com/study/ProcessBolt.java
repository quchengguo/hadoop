package com.study;

import com.study.utils.LogAnalyzerHandler;
import org.apache.storm.topology.*;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

/**
 * Created by quchengguo on 2018/7/7.
 * 计算一些网站的分析指标，比如pv，uv
 * 将数据保存到redis中
 */
public class ProcessBolt extends BaseBasicBolt {
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        // 1.获取上游发送的数据，得到一个message对象
        // 2.从数据库中加载任务信息，开始匹配计算
        // 3.将匹配到的任务结果保存到redis中

        Message message = (Message)tuple.getValueByField("message");
        LogAnalyzerHandler.process(message);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
