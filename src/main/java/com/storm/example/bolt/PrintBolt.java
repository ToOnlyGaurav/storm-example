package com.storm.example.bolt;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrintBolt extends BaseBasicBolt {
    private static final Logger LOG = LoggerFactory.getLogger(PrintBolt.class);
    private int count=0;

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String key = tuple.getStringByField("key");
        String value = tuple.getStringByField("value");
        System.out.println("count:" + count + ":" + value);
        count++;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

}
