package com.storm.example;

import com.storm.example.bolt.ReportBolt;
import com.storm.example.bolt.SplitSentenceBolt;
import com.storm.example.bolt.WordCountBolt;
import com.storm.example.spout.SentenceSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

public class WordCountTopology extends AbstractTopology {

    public WordCountTopology(String topologyName, Config conf) {
        super(topologyName, conf);
    }
    public WordCountTopology(Config conf) {
        super("word-count", conf);
    }

    public static void main(String[] args) {
        Config conf = new Config();
        conf.setNumEventLoggers(1);
        conf.setDebug(true);


        WordCountTopology topology = new WordCountTopology(conf);
        topology.init();
        StormTopology stormTopology = topology.build();

        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("word", conf, stormTopology);
//
//        Utils.sleep(10000);
//        localCluster.killTopology("word");
//        localCluster.shutdown();
    }

    @Override
    public void init() {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new SentenceSpout(), 1);
        builder.setBolt("split", new SplitSentenceBolt(), 1).shuffleGrouping("spout");
        builder.setBolt("count", new WordCountBolt(), 1).shuffleGrouping("split");
        builder.setBolt("report", new ReportBolt(), 1).shuffleGrouping("count");
    }

    @Override
    public StormTopology build() {
        return builder.createTopology();
    }
}
