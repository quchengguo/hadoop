package com.study;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

/**
 * Created by quchengguo on 2018/7/7.
 *任务提交类
 */
public class LogAnalyzerTopology {
    public static void main(String[] args) {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("KafkaSpout", new MySpout());
        topologyBuilder.setBolt("etlBolt", new EtlBolt()).localOrShuffleGrouping("KafkaSpout");
        topologyBuilder.setBolt("processBolt", new ProcessBolt()).localOrShuffleGrouping("etlBolt");

        Config config = new Config();
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("loganalyzer", config, topologyBuilder.createTopology());
    }
}
