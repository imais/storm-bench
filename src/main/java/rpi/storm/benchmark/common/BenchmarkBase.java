package rpi.storm.benchmark.common;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import yahoo.benchmark.common.Utils;

import java.util.List;
import java.util.Map;
import java.util.UUID;


abstract public class BenchmarkBase {
    private static final Logger log = LoggerFactory.getLogger(BenchmarkBase.class);

    private Config stormConf_;
    protected Map globalConf_;
    protected SpoutConfig spoutConf_;
    protected int spouts_parallel_;
    protected int bolts_parallel_;

    public BenchmarkBase(String args[]) throws ParseException {
        // Cli parameters (cmd.getOptionValue) have priorities over 
        // config file parameters (globalConf)
        Options opts = new Options();
        opts.addOption("conf", true, "Path to the config file.");
        opts.addOption("topic", true, "Kafka topic to consume.");
        opts.addOption("spouts_parallel", true, 
                       "Parallelism for spouts (= number of Kafka partitions)");
        opts.addOption("bolts_parallel", true, "Parallelism for bolts");
        opts.addOption("workers", true, "Number of workers.");
        opts.addOption("ackers", true, "Number of ackers.");
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(opts, args);
        String configPath = cmd.getOptionValue("conf");
        if (configPath == null) {
            log.error("Null config path");
            System.exit(1);
        }
        globalConf_ = Utils.findAndReadConfigFile(configPath, true);
        stormConf_ = new Config();

        // topic & kafkaSpout
        String topic = cmd.getOptionValue("topic");
        if (topic != null) globalConf_.put("kafka.topic", topic);
        topic = getConfString(globalConf_, "kafka.topic");
        String zkServerHosts = Utils.joinHosts(
            (List<String>)globalConf_.get("zookeeper.servers"),
            Integer.toString((Integer)globalConf_.get("zookeeper.port")));
        spoutConf_ = new SpoutConfig(new ZkHosts(zkServerHosts), 
                                     topic, "/" + topic, 
                                     UUID.randomUUID().toString());
        spoutConf_.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConf_.ignoreZkOffsets = true; // Read from the beginning of the topic

        // spouts parallelism
        String spouts_parallel = cmd.getOptionValue("spouts_parallel");
        if (spouts_parallel != null) globalConf_.put("storm.spouts_parallel", 
                                                     Integer.parseInt(spouts_parallel));
        spouts_parallel_ = getConfInt(globalConf_, "storm.spouts_parallel");

        // bolts parallelism
        String bolts_parallel = cmd.getOptionValue("bolts_parallel");
        if (bolts_parallel != null) globalConf_.put("storm.bolts_parallel", 
                                                    Integer.parseInt(bolts_parallel));
        bolts_parallel_ = getConfInt(globalConf_, "storm.bolts_parallel");

        // workers
        String workers = cmd.getOptionValue("workers");
        if (workers != null) globalConf_.put("storm.workers", Integer.parseInt(workers));
        stormConf_.setNumWorkers(getConfInt(globalConf_, "storm.workers"));

        // ackers
        String ackers = cmd.getOptionValue("ackers");
        if (ackers != null) globalConf_.put("storm.ackers", Integer.parseInt(ackers));
        stormConf_.setNumAckers(getConfInt(globalConf_, "storm.ackers"));

        // maxSpoutPendng
        int maxSpoutPending = getConfInt(globalConf_, "max.spout.pending");
        if (0 < maxSpoutPending)
            stormConf_.setMaxSpoutPending(maxSpoutPending);
    }

    abstract public StormTopology getTopology();

    public void submitTopology(String name) throws 
        AuthorizationException, AlreadyAliveException, InvalidTopologyException {
        StormSubmitter.submitTopologyWithProgressBar(name, stormConf_, getTopology());
    }

    public void submitTopology(String name, boolean local) throws 
        AuthorizationException, AlreadyAliveException, InvalidTopologyException {
        if (local) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(name, stormConf_, getTopology());
            backtype.storm.utils.Utils.sleep(10000);
            cluster.killTopology(name);
            cluster.shutdown();
        }
        else 
            submitTopology(name);
    }

    public static int getConfInt(Map conf, String field) {
        Object val = conf.get(field);
        if (val != null) {
            log.info(field + ": " + val);
            return ((Number)val).intValue();
        } 
        else {
            log.info(field + " not found");
            return -1;
        }
    }

    public static String getConfString(Map conf, String field) {
        Object val = conf.get(field);
        if (val != null) {
            log.info(field + ": " + val);
            return (String)val;
        } 
        else {
            log.info(field + " not found");
            return null;
        }
    }

    public static boolean getConfBoolean(Map conf, String field) {
        Object val = conf.get(field);
        if (val != null) {
            log.info(field + ": " + val);
            return Boolean.parseBoolean(String.valueOf(val));
        } 
        else {
            log.info(field + " not found");
            return false;
        }
    }
}
