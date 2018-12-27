package com.storm.example;

import com.storm.example.bolt.PrintBolt;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Properties;

public class KafkaReaderTopology {
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", getKafkaSpout(), 1);
        builder.setBolt("report", new PrintBolt(), 1).shuffleGrouping("spout");

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

    private static IRichSpout getKafkaSpout() {
        KafkaSpoutConfig<String, String> kafkaSpoutConfig = createSpoutConfig();
        KafkaSpout<String, String> kafkaSpout = new KafkaSpout<>(kafkaSpoutConfig);
        return kafkaSpout;
    }

    public static KafkaSpoutConfig<String, String> createSpoutConfig() {

        Properties props = new Properties();
        props.put("bootstrap.servers", "127.0.0.1:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");


        return KafkaSpoutConfig.builder(props.getProperty("bootstrap.servers"),  "hello")
                .setProp(props)
                .setProp(ConsumerConfig.GROUP_ID_CONFIG, "test")
                .setProp(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true)
//                .setFirstPollOffsetStrategy(EARLIEST)
                .setRecordTranslator((r) ->
                        new Values(r.key(), r.value()),
                        new Fields("key", "value"))
                .setPollTimeoutMs(1000).build();
    }
}
