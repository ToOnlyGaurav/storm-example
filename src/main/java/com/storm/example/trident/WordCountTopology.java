package com.storm.example.trident;

import com.storm.example.spout.SentenceSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.fluent.GroupedStream;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.Debug;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.testing.Split;
import org.apache.storm.tuple.Fields;

public class WordCountTopology {
    public static void main(String[] args) {
        TridentTopology topology = new TridentTopology();

        Stream spoutStream = topology.newStream("spout1", new SentenceSpout())
                .parallelismHint(1);

        GroupedStream words = spoutStream.each(new Fields("sentence"), new Split(), new Fields("word"))
                .parallelismHint(1)
                .groupBy(new Fields("word"));

        Stream counts = words.persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
                .parallelismHint(1)
                .newValuesStream();

        counts.each(new Fields("word", "count"), new Debug(true))
                .parallelismHint(1);

        StormTopology stormTopology = topology.build();

        Config conf = new Config();
        conf.setNumEventLoggers(1);
        conf.setDebug(true);

        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("word", conf, stormTopology);

    }
}
