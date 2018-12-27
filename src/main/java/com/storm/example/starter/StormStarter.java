package com.storm.example.starter;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.esotericsoftware.minlog.Log;
import com.storm.example.AbstractTopology;
import com.storm.example.conf.Configuration;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.util.Properties;

public class StormStarter {
    private static final Logger LOG = LoggerFactory.getLogger(StormStarter.class);
    private static final String LOCAL = "LOCAL";
    private static final String REMOTE = "REMOTE";

    @Parameter(names = {"-tc", "--topologyClass"}, description = "The Topology class to be executed", required = true)
    public String topologyClass;

    @Parameter(names = {"-p", "--property-file"}, description = "Property file for topology", required = true)
    public String configFile;

    @Parameter(names = {"-m", "--mode"}, description = "topology running mode")
    public String mode = LOCAL;

    @Parameter(names = {"-tn", "--topology-name"}, description = "The name of the topology")
    public String topologyName;

    @Parameter(names = {"-r", "--runtime"}, description = "Runtime in seconds for the topology (local mode only)")
    public int runtimeInSeconds = 300;

    public void start() throws InterruptedException, AlreadyAliveException, InvalidTopologyException, AuthorizationException, IOException {
        Properties properties;
        try {
            properties = loadProperties(configFile);
        } catch (IOException e) {
            Log.error("Unable to read properties file.");
            throw e;
        }
        Configuration config = Configuration.fromProperties(properties);
        AbstractTopology abstractTopology;
        try {
            Class cls = Class.forName(topologyClass);
            Constructor<AbstractTopology> constructor = cls.getConstructor(String.class, Config.class);
            abstractTopology = constructor.newInstance(topologyName, config);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("Unable to create topology");
        }

        abstractTopology.init();
        StormTopology stormTopology = abstractTopology.build();

        switch (mode) {
            case LOCAL:
                runTopologyLocally(stormTopology, topologyName, config, runtimeInSeconds);
                break;
            case REMOTE:
                runTopologyRemotely(stormTopology, topologyName, config);
                break;
            default:
                throw new RuntimeException("Not a valid option");
        }
    }

    public static void main(String[] args) {
        StormStarter runner = new StormStarter();
        JCommander cmd = new JCommander(runner);

        try {
            cmd.parse(args);
        } catch (ParameterException ex) {
            LOG.error("Unable to parse arguments", ex);
            cmd.usage();
            System.exit(1);
        }

        try {
            runner.start();
        } catch (Exception ex) {
            LOG.error("Exception wile running topology", ex);
            System.exit(1);
        }
    }

    private static void runTopologyLocally(StormTopology topology, String topologyName,
                                           Config conf, int runtimeInSeconds) throws InterruptedException {
        LOG.info("Starting Storm on local mode to start for {} seconds", runtimeInSeconds);
        LocalCluster cluster = new LocalCluster();

        LOG.info("Topology {} submitted", topologyName);
        cluster.submitTopology(topologyName, conf, topology);
        Thread.sleep((long) runtimeInSeconds * 1000);

        cluster.killTopology(topologyName);
        LOG.info("Topology {} finished", topologyName);

        cluster.shutdown();
        LOG.info("Local Storm cluster was shutdown", topologyName);
    }

    public static void runTopologyRemotely(StormTopology topology, String topologyName,
                                           Config conf) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
        StormSubmitter.submitTopology(topologyName, conf, topology);
    }

    public static Properties loadProperties(String filename) throws IOException {
        Properties properties = new Properties();
        InputStream is;
        is = StormStarter.class.getResourceAsStream(filename);
        if (is == null) {
            is = new FileInputStream(filename);
        }

        properties.load(is);
        is.close();

        return properties;
    }
}
