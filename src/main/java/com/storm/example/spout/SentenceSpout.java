package com.storm.example.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SentenceSpout extends BaseRichSpout {
    private String[] sentences = {
            "This is a storm example",
            "Storm is very fast",
            "strom is a batch processing system"
    };
    private int index = 0;
    private SpoutOutputCollector spoutOutputCollector;
    private static final Logger LOG = LoggerFactory.getLogger(SentenceSpout.class);

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
//        Utils.sleep(100);
        index = index % sentences.length;
        String sentence = sentences[index++];
        LOG.debug("Emitting tuple: {}", sentence);
        this.spoutOutputCollector.emit(new Values(sentence));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("sentence"));
    }
}
