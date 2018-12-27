package com.storm.example;

import com.storm.example.bolt.ReportBolt;
import com.storm.example.bolt.SplitSentenceBolt;
import com.storm.example.bolt.WordCountBolt;
import com.storm.example.spout.SentenceSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class WordCountTopology {
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new SentenceSpout(), 1);
        builder.setBolt("split", new SplitSentenceBolt(), 1).shuffleGrouping("spout");
        builder.setBolt("count", new WordCountBolt(), 1).shuffleGrouping("split");
        builder.setBolt("report", new ReportBolt(), 1).shuffleGrouping("count");

        Config conf = new Config();
        conf.setNumEventLoggers(1);
        conf.setDebug(true);

        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("word", conf, builder.createTopology());
//
//        Utils.sleep(10000);
//        localCluster.killTopology("word");
//        localCluster.shutdown();
    }

}
