package com.study.utils;

import com.study.Message;
import com.study.dao.LogAnalyzerDao;
import com.study.dao.mapper.LogAnalyzerMapper;
import com.study.domain.LogAnalyzeJob;
import com.study.domain.LogAnalyzeJobDetail;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by quchengguo on 2018/7/7.
 */
public class LogAnalyzerHandler {
    private static HashMap<String, List<LogAnalyzeJob>> jobHashMap;
    private static HashMap<String, List<LogAnalyzeJobDetail>> detailHashMap;

    static {
        try {
            loadData();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void loadData() throws IOException {
        // 注意这里是mybatis的配置文件
        SqlSessionFactory factory = new SqlSessionFactoryBuilder().build(Resources.getResourceAsStream("SqlMapConfig.xml"));
        SqlSession sqlSession = factory.openSession();
        LogAnalyzerMapper logAnalyzerMapper = sqlSession.getMapper(LogAnalyzerMapper.class);

        List<LogAnalyzeJob> logAnalyzeJobs = logAnalyzerMapper.findAllJobList();
        List<LogAnalyzeJobDetail> logAnalyzeJobDetails = logAnalyzerMapper.findJobDetailList();
        // 每一种日志类型的任务单独分开
        //以任务类型为key，以任务的Job为List<Job>
        jobHashMap = new HashMap<String, List<LogAnalyzeJob>>();
        for (LogAnalyzeJob analyzeJob : logAnalyzeJobs) {
            int jobType = analyzeJob.getJobType();
            List<LogAnalyzeJob> jobList = jobHashMap.get(jobType + "");
            if (jobList == null) {
                jobList = new ArrayList<LogAnalyzeJob>();
            }
            jobList.add(analyzeJob);
            jobHashMap.put(jobType + "", jobList);
        }
        // 每个job和job的判断条件成一个map
        // jobid为key，所有的组成的list为value
        detailHashMap = new HashMap<String, List<LogAnalyzeJobDetail>>();
        for (LogAnalyzeJobDetail analyzeJobDetail : logAnalyzeJobDetails) {
            int jobId = analyzeJobDetail.getJobId();
            List<LogAnalyzeJobDetail> detailList = detailHashMap.get(jobId + "");
            if (detailList == null) {
                detailList = new ArrayList<LogAnalyzeJobDetail>();
            }
            detailList.add(analyzeJobDetail);
            detailHashMap.put(jobId + "", detailList);
        }
    }

    /**
     * 1. 获取上游发送的数据 得到一个message对象
     * 2.从数据库中加载任务信息，开始匹配并计算
     * 3.将匹配到的任务结果保存到redis中
     */
    public static void process(Message message) {
        // 1.获取message中任务的类型
        String type = message.getType();
        // 2.通过type获取这个消息的所有任务信息
        List<LogAnalyzeJob> jobList = jobHashMap.get(type);
        // 3.迭代每个job，看看是否被触发
        for (LogAnalyzeJob logAnalyzeJob : jobList) {
            // 继续获取job所有的条件
            String jobId = logAnalyzeJob.getJobId();
            List<LogAnalyzeJobDetail> detailList = detailHashMap.get(jobId);
            // 条件中，必须所有的都被满足才会触发这个任务
            boolean isMatch = false;
            for (LogAnalyzeJobDetail logAnalyzeJobDetail : detailList) {
                // 如果一个条件不满足，就中断这个job所有条件的判断
                String field = logAnalyzeJobDetail.getField();
                String compareValue = null;
                try {
                    compareValue = message.getCompareValue(field);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                // 用户在后台系统输入到数据库一个值
                String inputValue = logAnalyzeJobDetail.getValue();
                // 比较的方式 1包含 2等于
                int compare = logAnalyzeJobDetail.getCompare();
                if (compare == 1 && compareValue.contains(inputValue)) {
                    //规则就满足了
                    isMatch = true;
                }
                if (compare == 2 && compareValue.equals(inputValue)) {
                    // 规则就满足了
                    isMatch = true;
                }
                if (!isMatch) {
                    return;
                }
            }
            Jedis jedis = RedisPool.getJedis();
            try {
                //如果一个job的所有任务都满足了。
                // pv 来一条消息就算一条
                String pvKey = "loganalyzer:" + jobId + ":pv:20190808";
                jedis.incr(pvKey);
                // uv 使用用户id去重
                String uvKey = "loganalyzer:" + jobId + ":uv:20190808";
                jedis.sadd(uvKey, message.getUser());
            } finally {
                jedis.close();
            }
        }
    }
}
