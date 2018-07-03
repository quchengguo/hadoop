package com.study;

import lombok.extern.slf4j.Slf4j;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import redis.clients.jedis.Jedis;

import java.util.Map;

/**
 *计算相关的指标
 * 计算那些指标？
 * 计算订单金额（payPrice字段的金额进行累加）、订单数（一条消息就是一个订单）、订单人数（一个消息就是一个人）
 * -------------指标口径的统一，在每个企业中都不一样。作为开发者，一定要找产品经理或者提需求的人讨论明白。
 * 指标数据存放到哪里？
 * --------------存放到redis
 */
@Slf4j
public class PaymentIndexProcessBolt extends BaseRichBolt{
    private Jedis jedis = null;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        jedis = new Jedis("node1",6379);
    }

    @Override
    public void execute(Tuple inputTuple) {
        // 获取上游发送的字段
        Payment value = (Payment) inputTuple.getValue(0);

        //  计算订单金额（payPrice字段的金额进行累加）、订单数（一条消息就是一个订单）、订单人数（一个消息就是一个人）
        jedis.incrBy("price", value.getPayPrice());
        jedis.incrBy("orderNum", 1);
        jedis.incrBy("orderUser", 1);

        // 计算每个品类的订单数，订单人数，订单金额
        // 返回的是三品的一级品类，二级品类，三级品类
        String categorys = value.getCatagorys();
        String[] split = categorys.split(",");
        for(int i = 0; i < split.length; i++){
            jedis.incrBy("price:"+(i+1)+":"+split[i], value.getPayPrice());
            jedis.incrBy("orderNum:"+(i+1)+":"+split[i],1);
            jedis.incrBy("orderUser:"+(i+1)+":"+split[i],1);
        }

        // 计算商品属于哪个产品线
//        jedis.incrBy("price:cpxid:"+split[i], value.getPayPrice());
        //计算每个店铺的订单人数，订单金额，订单数
        jedis.incrBy("price:shop:"+value.getShopId(), value.getPayPrice());
        jedis.incrBy("orderNum:shop:"+value.getShopId(),1);
        jedis.incrBy("orderUser:shop:"+value.getShopId(),1);

        //计算每个商品的订单人数，订单金额，订单数
        jedis.incrBy("price:pid:"+value.getProductId(), value.getPayPrice());
        jedis.incrBy("orderNum:pid:"+value.getProductId(),1);
        jedis.incrBy("orderUser:pid:"+value.getProductId(),1);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
