package rpi.storm.benchmark;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
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
import intel.storm.benchmark.lib.bolt.PageViewBolt;
import intel.storm.benchmark.lib.bolt.UniqueVisitorBolt;
import static intel.storm.benchmark.lib.spout.pageview.PageView.Item;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.List;


public class UniqueVisitor {
    private static final Logger log = LoggerFactory.getLogger(UniqueVisitor.class);

    public static final String SPOUT_ID = "spout";
    public static final String VIEW_ID = "view";
    public final static String UNIQUER_ID = "uniquer";

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

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
        int winLen = ((Number)commonConfig.get("uniquevisitor.window_length")).intValue();
        int emitFreq = ((Number)commonConfig.get("uniquevisitor.emit_freq")).intValue();

        ZkHosts hosts = new ZkHosts(zkServerHosts);

        SpoutConfig spoutConfig = new SpoutConfig(hosts, kafkaTopic, "/" + kafkaTopic, UUID.randomUUID().toString());
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

        builder.setSpout(SPOUT_ID, kafkaSpout, parallel);
        builder.setBolt(VIEW_ID, new PageViewBolt(Item.URL, Item.USER), parallel)
            .localOrShuffleGrouping(SPOUT_ID);
        builder.setBolt(UNIQUER_ID, new UniqueVisitorBolt(winLen, emitFreq), parallel)
            .fieldsGrouping(VIEW_ID, new Fields(Item.URL.toString()));
        
        Config conf = new Config();

        log.info("Topology started");

        if (args != null && args.length > 0) {
            conf.setNumWorkers(workers);
            conf.setNumAckers(ackers);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        }
        else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
            backtype.storm.utils.Utils.sleep(10000);
            cluster.killTopology("test");
            cluster.shutdown();
        }
    }
}
