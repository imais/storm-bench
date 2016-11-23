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

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class Grep {
    private static final Logger log = LoggerFactory.getLogger(Grep.class);

    public static final String SPOUT_ID = "spout";
    public static final String FM_ID = "find";
    public static final String CM_ID = "count";

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
        String ptnString = (String)commonConfig.get("grep.pattern_string");

        ZkHosts hosts = new ZkHosts(zkServerHosts);

        SpoutConfig spoutConfig = new SpoutConfig(hosts, kafkaTopic, "/" + kafkaTopic, UUID.randomUUID().toString());
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

        builder.setSpout(SPOUT_ID, kafkaSpout, parallel);
        builder.setBolt(FM_ID, new FindMatchingSentence(ptnString), parallel)
            .localOrShuffleGrouping(SPOUT_ID);
        builder.setBolt(CM_ID, new CountMatchingSentence(), parallel)
            .fieldsGrouping(FM_ID, new Fields(FindMatchingSentence.FIELDS));

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

    public static class FindMatchingSentence extends BaseBasicBolt {
        public static final String FIELDS = "word";
        private Pattern pattern;
        private Matcher matcher;
        private final String ptnString;

        public FindMatchingSentence(String ptnString) {
            this.ptnString = ptnString;
        }

        @Override
        public void prepare(Map stormConf, TopologyContext context) {
            pattern = Pattern.compile(ptnString);
        }

        @Override
        public void execute(Tuple input, BasicOutputCollector collector) {
            String sentence = input.getString(0);
            matcher = pattern.matcher(input.getString(0));
            if (matcher.find()) {
                log.debug(String.format("find pattern %s in sentence %s", ptnString, sentence));
                collector.emit(new Values(1));
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields(FIELDS));
        }
    }

    public static class CountMatchingSentence extends BaseBasicBolt {
        public static final String FIELDS = "count";
        private int count = 0;

        @Override
        public void execute(Tuple input, BasicOutputCollector collector) {
            if (input.getInteger(0).equals(1)) {
                collector.emit(new Values(count++));
                log.debug("count: " + count);
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields(FIELDS));
        }
    }
}
