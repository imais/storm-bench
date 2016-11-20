package rpi.storm.benchmark;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.kafka.ZkHosts;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.spout.IPartitionedTridentSpout;
import storm.trident.testing.MemoryMapState;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import yahoo.benchmark.common.Utils;
import intel.storm.benchmark.lib.operation.WordSplit;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.List;


public class TridentWordCount {
    private static final Logger log = LoggerFactory.getLogger(TridentWordCount.class);

    public static final String SPOUT_ID = "spout";
    public static final String SPLIT_ID = "split";
    public static final String COUNT_ID = "count";

    public static void main(String[] args) throws Exception {
        Options opts = new Options();
        opts.addOption("conf", true, "Path to the config file.");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(opts, args);
        String configPath = cmd.getOptionValue("conf");
        Map commonConfig = Utils.findAndReadConfigFile(configPath, true);
        String zkServerHosts = Utils.joinHosts((List<String>)commonConfig.get("zookeeper.servers"),
                                               Integer.toString((Integer)commonConfig.get("zookeeper.port")));
        String kafkaTopic = (String)commonConfig.get("kafka.topic");
        int parallel = ((Number)commonConfig.get("kafka.partitions")).intValue();
        int workers = ((Number)commonConfig.get("storm.workers")).intValue();
        int ackers = ((Number)commonConfig.get("storm.ackers")).intValue();
        int cores = ((Number)commonConfig.get("process.cores")).intValue();

        ZkHosts hosts = new ZkHosts(zkServerHosts);

        TridentKafkaConfig tridentConf = new TridentKafkaConfig(hosts, 
                                                                kafkaTopic, 
                                                                UUID.randomUUID().toString());
        tridentConf.scheme = new SchemeAsMultiScheme(new StringScheme());
        IPartitionedTridentSpout spout = new TransactionalTridentKafkaSpout(tridentConf);

        TridentTopology trident = new TridentTopology();
        trident.newStream("wordcount", spout).name("sentence").parallelismHint(parallel).shuffle()
            .each(new Fields(StringScheme.STRING_SCHEME_KEY), new WordSplit(), new Fields("word"))
            .parallelismHint(parallel)
            .groupBy(new Fields("word"))
            .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
            .parallelismHint(parallel);

        Config conf = new Config();

        log.info("Topology started");

        if (args != null && args.length > 0) {
            conf.setNumWorkers(workers);
            conf.setNumAckers(ackers);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, trident.build());
        }
        else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, trident.build());
            backtype.storm.utils.Utils.sleep(10000);
            cluster.killTopology("test");
            cluster.shutdown();
        }
    }
}