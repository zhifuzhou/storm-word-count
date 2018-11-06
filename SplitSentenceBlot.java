package com.suning.wordcounts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SplitSentenceBlot extends BaseBasicBolt {


    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String word = tuple.getString(0);
        String[] words = word.split(" ");
        for (String s : words) {
            System.out.println("==========" + s);
            collector.emit(new Values(s));
        }
    }


    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}
