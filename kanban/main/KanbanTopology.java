package com.study;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;

/**
 * Created by cheng on 2018/7/3.
 * 驱动类
 * 提交一个订单实时金额统计的任务
 *
 * spout的数量应该和partition的数量应该一致
 */
public class KanbanTopology {
    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        // 1.设置一个kafkaspout消费kafka集群中的数据
        KafkaSpoutConfig.Builder<String, String> builder = KafkaSpoutConfig.builder("node1:9092", "payment");
        builder.setGroupId("payment_storm_index");
        KafkaSpoutConfig<String, String> kafkaSpoutConfig = builder.build();

        topologyBuilder.setSpout("KafkaSpout", new KafkaSpout<String, String>(kafkaSpoutConfig), 3);

        // 2.设置解析json串的bolt
        topologyBuilder.setBolt("ParserPaymentBolt", new ParserPaymentBolt(),3).shuffleGrouping("KafkaSpout");

        // 3.设置计算指标的bolt
        topologyBuilder.setBolt("PaymentIndexProcessBolt", new PaymentIndexProcessBolt(),3).shuffleGrouping("ParserPaymentBolt");

        // 4.得到一个真正的stromtopology对象，用来提交到集群
        StormTopology topology = topologyBuilder.createTopology();

        // 5.使用本地模式运行任务
        Config config = new Config();
        config.setNumWorkers(1);
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("kanban", config, topology);

        // 提交到集群
//        StormSubmitter.submitTopology("kanban", config, topology);


    }
}
