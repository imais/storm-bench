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
import intel.storm.benchmark.util.TupleHelpers;

import java.io.Serializable;
import java.util.*;


public class RollingSort {
    private static final Logger log = LoggerFactory.getLogger(RollingSort.class);

    public static final String SPOUT_ID = "spout";
    public static final String SORT_BOLT_ID ="sort";

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
        int emitFreq = ((Number)commonConfig.get("rollingsort.emit_freq")).intValue();
        int chunkSize = ((Number)commonConfig.get("rollingsort.chunk_size")).intValue();

        ZkHosts hosts = new ZkHosts(zkServerHosts);

        SpoutConfig spoutConfig = new SpoutConfig(hosts, kafkaTopic, "/" + kafkaTopic, UUID.randomUUID().toString());
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        KafkaSpout spout = new KafkaSpout(spoutConfig);

        builder.setSpout(SPOUT_ID, spout, parallel);
        builder.setBolt(SORT_BOLT_ID, new SortBolt(emitFreq, chunkSize), parallel)
            .localOrShuffleGrouping(SPOUT_ID);
        
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


    public static class SortBolt extends BaseBasicBolt {

        public static final String EMIT_FREQ = "emit.frequency";
        public static final int DEFAULT_EMIT_FREQ = 60;  // 60s
        public static final String CHUNK_SIZE = "chunk.size";
        public static final int DEFAULT_CHUNK_SIZE = 100;
        public static final String FIELDS = "sorted_data";

        private final int emitFrequencyInSeconds;
        private final int chunkSize;
        private int index = 0;
        private MutableComparable[] data;


        public SortBolt(int emitFrequencyInSeconds, int chunkSize) {
            this.emitFrequencyInSeconds = emitFrequencyInSeconds;
            this.chunkSize = chunkSize;
        }

        @Override
        public void prepare(Map stormConf, TopologyContext context) {
            this.data = new MutableComparable[this.chunkSize];
            for (int i = 0; i < this.chunkSize; i++) {
                this.data[i] = new MutableComparable();
            }
        }

        @Override
        public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
            if (TupleHelpers.isTickTuple(tuple)) {
                Arrays.sort(data);
                basicOutputCollector.emit(new Values(data));
                log.info("index = " + index);
            } else {
                Object obj = tuple.getValue(0);
                if (obj instanceof Comparable) {
                    data[index].set((Comparable) obj);
                } else {
                    throw new RuntimeException("tuple value is not a Comparable");
                }
                index = (index + 1 == chunkSize) ? 0 : index + 1;
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields(FIELDS));
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            Map<String, Object> conf = new HashMap<String, Object>();
            conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);
            return conf;
        }
    }

    private static class MutableComparable implements Comparable, Serializable {
        private static final long serialVersionUID = -5417151427431486637L;
        private Comparable c = null;

        public MutableComparable() {

        }

        public MutableComparable(Comparable c) {
            this.c = c;
        }

        public void set(Comparable c) {
            this.c = c;
        }

        public Comparable get() {
            return c;
        }

        @Override
        public int compareTo(Object other) {
            if (other == null) return 1;
            Comparable oc = ((MutableComparable) other).get();
            if (null == c && null == oc) {
                return 0;
            } else if (null == c) {
                return -1;
            } else if (null == oc) {
                return 1;
            } else {
                return c.compareTo(oc);
            }
        }
    }
}
