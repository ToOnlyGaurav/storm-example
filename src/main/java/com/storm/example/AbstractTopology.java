package com.storm.example;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

public abstract class AbstractTopology {
    protected TopologyBuilder builder;
    protected String topologyName;
    protected Config config;

    public AbstractTopology(String topologyName, Config config) {
        this.topologyName = topologyName;
        this.config = config;
        builder = new TopologyBuilder();
    }

    public abstract void init();

    public StormTopology build() {
        return builder.createTopology();
    }
}
