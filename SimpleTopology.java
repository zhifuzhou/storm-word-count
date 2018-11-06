package com.suning.wordcounts;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class SimpleTopology {
    public static void main(String[] args) throws Exception{
        // 实例化TopologyBuilder类。
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        // 设置喷发节点并分配并发数，该并发数将会控制该对象在集群中的线程数。
        topologyBuilder.setSpout("spout", new RandomSentenceSpout(), 1);
        // 设置数据处理节点并分配并发数。指定该节点接收喷发节点的策略为随机方式。
        topologyBuilder.setBolt("split", new SplitSentenceBlot(), 3).shuffleGrouping("spout");
        topologyBuilder.setBolt("count", new WordCountBlot(), 3).fieldsGrouping("split", new Fields("word"));
        String topologyName = "word-count";

        Config config = new Config();
//        config.setDebug(true);
        if (args != null && args.length > 0) {
            config.setNumWorkers(1);
            StormSubmitter.submitTopology(args[0], config, topologyBuilder.createTopology());
        } else {
            // 这里是本地模式下运行的启动代码。
            config.setMaxTaskParallelism(1);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(topologyName, config, topologyBuilder.createTopology());
        }
    }
}
